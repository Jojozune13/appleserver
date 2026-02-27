require('dotenv').config();
console.log('Environment variables loaded?', process.env.FIREBASE_PROJECT_ID ? 'YES ✅' : 'NO ❌');
const WebSocket = require('ws');
const admin = require('firebase-admin');
const jwt = require('jsonwebtoken');
const http = require('http');
const url = require('url');

// JWT Secret (use environment variable or generate one)
const JWT_SECRET = process.env.JWT_SECRET || 'your-super-secret-jwt-key-change-in-production';
const TOKEN_EXPIRY = '30d'; // Token expires in 30 days

// Initialize Firebase with environment variables
const serviceAccount = {
  projectId: process.env.FIREBASE_PROJECT_ID,
  privateKey: process.env.FIREBASE_PRIVATE_KEY.replace(/\\n/g, '\n'),
  clientEmail: process.env.FIREBASE_CLIENT_EMAIL
};

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: process.env.FIREBASE_DATABASE_URL
});

const db = admin.firestore();

// Collections
const tagsCollection = db.collection('active_tags');
const connectionsCollection = db.collection('connections');
const messagesCollection = db.collection('messages');

// Create HTTP server
const server = http.createServer();
const wss = new WebSocket.Server({ server });

// In-memory storage for active connections
const activeConnections = new Map(); // socket -> { tag, connectionId, lastSeen, ip, token }
const ipConnectionCount = new Map(); // ip -> count (for rate limiting)

// Rate limiting config
const RATE_LIMIT_WINDOW = parseInt(process.env.RATE_LIMIT_WINDOW) || 60000;
const MAX_CONNECTIONS_PER_IP = parseInt(process.env.MAX_CONNECTIONS_PER_IP) || 5;

// Handle connections
wss.on('connection', async (ws, req) => {
  const ip = req.socket.remoteAddress;
  console.log(`New connection from ${ip}`);

  // Rate limiting check
  const currentConnections = ipConnectionCount.get(ip) || 0;
  if (currentConnections >= MAX_CONNECTIONS_PER_IP) {
    console.log(`Rate limit exceeded for ${ip}`);
    ws.close(1008, 'Too many connections from this IP');
    return;
  }

  // Increment connection count
  ipConnectionCount.set(ip, currentConnections + 1);

  // Generate unique connection ID
  const connectionId = generateConnectionId();
  
  // Generate JWT token for this device
  const token = jwt.sign(
    { connectionId, ip, iat: Date.now() },
    JWT_SECRET,
    { expiresIn: TOKEN_EXPIRY }
  );
  
  // Store connection
  activeConnections.set(ws, {
    tag: null,
    connectionId,
    ip,
    token,
    lastSeen: Date.now(),
    connectedAt: Date.now()
  });

  // Send welcome message with token
  ws.send(JSON.stringify({
    type: 'connected',
    connectionId,
    token,
    message: 'Connected to AR Tag Server',
    timestamp: new Date().toISOString()
  }));

  // Log connection to Firebase
  try {
    await connectionsCollection.doc(connectionId).set({
      ip,
      connectedAt: admin.firestore.FieldValue.serverTimestamp(),
      userAgent: req.headers['user-agent'] || 'unknown'
    });
  } catch (error) {
    console.error('Error logging connection:', error);
  }

  // Handle incoming messages
  ws.on('message', async (data) => {
    try {
      const message = JSON.parse(data.toString());
      console.log(`[${connectionId}] Received:`, message.type);
      
      // Update last seen
      const connection = activeConnections.get(ws);
      if (connection) {
        connection.lastSeen = Date.now();
      }
      
      // Verify token if provided (optional - for security)
      if (message.token && connection && message.token !== connection.token) {
        ws.send(JSON.stringify({
          type: 'error',
          message: 'Invalid token'
        }));
        return;
      }

      switch (message.type) {
        case 'register_tag':
          await handleTagRegistration(ws, message.tag);
          break;
        case 'unregister_tag':
          await handleTagUnregistration(ws);
          break;
        case 'get_active_tags':
          await sendActiveTags(ws);
          break;
        case 'send_message':
          await handleMessage(ws, message, connection);
          break;
        case 'get_messages':
          await fetchMessages(ws, message.limit || 50);
          break;
        case 'ping':
          ws.send(JSON.stringify({ type: 'pong', timestamp: Date.now() }));
          break;
        default:
          ws.send(JSON.stringify({
            type: 'error',
            message: 'Unknown command'
          }));
      }
    } catch (error) {
      console.error('Error processing message:', error);
      ws.send(JSON.stringify({
        type: 'error',
        message: 'Invalid message format'
      }));
    }
  });

  // Handle disconnection
  ws.on('close', async () => {
    console.log(`Connection closed from ${ip}`);
    
    // Decrement IP connection count
    const count = ipConnectionCount.get(ip) || 1;
    ipConnectionCount.set(ip, Math.max(0, count - 1));

    const connection = activeConnections.get(ws);
    if (connection) {
      // If they had a tag, remove it
      if (connection.tag) {
        await removeTag(connection.tag, connection.connectionId);
      }

      // Update Firebase connection log
      try {
        await connectionsCollection.doc(connection.connectionId).update({
          disconnectedAt: admin.firestore.FieldValue.serverTimestamp()
        });
      } catch (error) {
        console.error('Error updating connection log:', error);
      }

      activeConnections.delete(ws);
    }
  });
});

// Handle tag registration
async function handleTagRegistration(ws, requestedTag) {
  const connection = activeConnections.get(ws);
  
  if (!connection) {
    ws.send(JSON.stringify({
      type: 'register_response',
      success: false,
      message: 'Connection not found'
    }));
    return;
  }

  // Validate tag
  if (!requestedTag || typeof requestedTag !== 'string') {
    ws.send(JSON.stringify({
      type: 'register_response',
      success: false,
      message: 'Invalid tag format'
    }));
    return;
  }

  // Sanitize tag (alphanumeric and underscores only)
  const sanitizedTag = requestedTag.replace(/[^a-zA-Z0-9_]/g, '');
  
  if (sanitizedTag.length < 2 || sanitizedTag.length > 20) {
    ws.send(JSON.stringify({
      type: 'register_response',
      success: false,
      message: 'Tag must be 2-20 characters (letters, numbers, underscores)'
    }));
    return;
  }

  try {
    // Check Firebase if tag is already active
    const tagDoc = await tagsCollection.doc(sanitizedTag).get();
    
    if (tagDoc.exists && tagDoc.data().active) {
      ws.send(JSON.stringify({
        type: 'register_response',
        success: false,
        message: 'Tag already in use',
        tag: sanitizedTag
      }));
      return;
    }

    // If this connection already has a tag, remove it first
    if (connection.tag) {
      await removeTag(connection.tag, connection.connectionId);
    }

    // Register new tag in Firebase
    await tagsCollection.doc(sanitizedTag).set({
      tag: sanitizedTag,
      connectionId: connection.connectionId,
      active: true,
      registeredAt: admin.firestore.FieldValue.serverTimestamp(),
      lastSeen: admin.firestore.FieldValue.serverTimestamp(),
      ip: connection.ip
    });

    // Update connection with new tag
    connection.tag = sanitizedTag;

    console.log(`Tag registered: ${sanitizedTag} (${connection.connectionId})`);

    // Send success response
    ws.send(JSON.stringify({
      type: 'register_response',
      success: true,
      message: 'Tag registered successfully',
      tag: sanitizedTag
    }));

    // Broadcast updated tag list to all connections
    await broadcastActiveTags();

  } catch (error) {
    console.error('Firebase error during registration:', error);
    ws.send(JSON.stringify({
      type: 'register_response',
      success: false,
      message: 'Server error during registration'
    }));
  }
}

// Handle tag unregistration
async function handleTagUnregistration(ws) {
  const connection = activeConnections.get(ws);
  
  if (connection && connection.tag) {
    await removeTag(connection.tag, connection.connectionId);
    connection.tag = null;
    
    ws.send(JSON.stringify({
      type: 'unregister_response',
      success: true,
      message: 'Tag unregistered'
    }));
  }
}

// Remove tag from Firebase
async function removeTag(tag, connectionId) {
  try {
    await tagsCollection.doc(tag).update({
      active: false,
      removedAt: admin.firestore.FieldValue.serverTimestamp()
    });
    
    console.log(`Tag removed: ${tag} (${connectionId})`);
    await broadcastActiveTags();
  } catch (error) {
    console.error('Error removing tag:', error);
  }
}

// Send active tags to a specific connection
async function sendActiveTags(ws) {
  try {
    const snapshot = await tagsCollection.where('active', '==', true).get();
    const tags = [];
    
    snapshot.forEach(doc => {
      tags.push(doc.data().tag);
    });

    ws.send(JSON.stringify({
      type: 'tag_list',
      tags: tags,
      count: tags.length,
      timestamp: Date.now()
    }));
  } catch (error) {
    console.error('Error fetching tags:', error);
    ws.send(JSON.stringify({
      type: 'error',
      message: 'Error fetching tags'
    }));
  }
}

// Broadcast active tags to all connections
async function broadcastActiveTags() {
  try {
    const snapshot = await tagsCollection.where('active', '==', true).get();
    const tags = [];
    
    snapshot.forEach(doc => {
      tags.push(doc.data().tag);
    });

    const message = JSON.stringify({
      type: 'tag_list',
      tags: tags,
      count: tags.length,
      timestamp: Date.now()
    });

    activeConnections.forEach((connection, client) => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(message);
      }
    });

    console.log(`Broadcasted ${tags.length} active tags`);
  } catch (error) {
    console.error('Error broadcasting tags:', error);
  }
}

// Handle incoming messages
async function handleMessage(ws, message, connection) {
  if (!message.text || typeof message.text !== 'string') {
    ws.send(JSON.stringify({
      type: 'message_response',
      success: false,
      message: 'Message text is required'
    }));
    return;
  }

  if (message.text.trim().length === 0) {
    ws.send(JSON.stringify({
      type: 'message_response',
      success: false,
      message: 'Message cannot be empty'
    }));
    return;
  }

  // Limit message length (e.g., 1000 characters)
  if (message.text.length > 1000) {
    ws.send(JSON.stringify({
      type: 'message_response',
      success: false,
      message: 'Message too long (max 1000 characters)'
    }));
    return;
  }

  try {
    // Create message object
    const messageData = {
      text: message.text.trim(),
      connectionId: connection.connectionId,
      token: connection.token,
      tag: connection.tag || 'anonymous',
      ip: connection.ip,
      sentAt: admin.firestore.FieldValue.serverTimestamp(),
      timestamp: Date.now()
    };

    // Store message in Firebase
    const docRef = await messagesCollection.add(messageData);

    console.log(`Message stored: ${docRef.id} from ${connection.connectionId}`);

    // Send confirmation to sender
    ws.send(JSON.stringify({
      type: 'message_response',
      success: true,
      message: 'Message sent',
      messageId: docRef.id
    }));

    // Broadcast message to all other connections
    await broadcastMessage(messageData, docRef.id, connection.connectionId);

  } catch (error) {
    console.error('Error storing message:', error);
    ws.send(JSON.stringify({
      type: 'message_response',
      success: false,
      message: 'Server error storing message'
    }));
  }
}

// Broadcast message to all other connections
async function broadcastMessage(messageData, messageId, senderConnectionId) {
  const broadcastPayload = JSON.stringify({
    type: 'new_message',
    messageId,
    text: messageData.text,
    from: messageData.tag,
    connectionId: messageData.connectionId,
    timestamp: messageData.timestamp
  });

  activeConnections.forEach((connection, client) => {
    // Send to all clients except the sender
    if (client.readyState === WebSocket.OPEN && connection.connectionId !== senderConnectionId) {
      client.send(broadcastPayload);
    }
  });

  console.log(`Broadcasted message to ${activeConnections.size - 1} other connections`);
}

// Fetch previous messages
async function fetchMessages(ws, limit) {
  try {
    const snapshot = await messagesCollection
      .orderBy('sentAt', 'desc')
      .limit(Math.min(limit, 100))
      .get();

    const messages = [];
    snapshot.forEach(doc => {
      const data = doc.data();
      messages.unshift({
        messageId: doc.id,
        text: data.text,
        from: data.tag,
        timestamp: data.timestamp,
        connectionId: data.connectionId
      });
    });

    ws.send(JSON.stringify({
      type: 'message_history',
      messages: messages,
      count: messages.length,
      timestamp: Date.now()
    }));
  } catch (error) {
    console.error('Error fetching messages:', error);
    ws.send(JSON.stringify({
      type: 'error',
      message: 'Error fetching message history'
    }));
  }
}

// Generate unique connection ID
function generateConnectionId() {
  return 'conn_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
}

// Verify JWT token
function verifyToken(token) {
  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    return decoded;
  } catch (error) {
    console.error('Token verification failed:', error.message);
    return null;
  }
}

// Clean up stale connections and IP counts
setInterval(() => {
  const now = Date.now();
  const timeout = 30000; // 30 seconds

  activeConnections.forEach((connection, ws) => {
    if (now - connection.lastSeen > timeout && ws.readyState === WebSocket.OPEN) {
      console.log(`Closing stale connection: ${connection.connectionId}`);
      ws.close(1000, 'Connection timeout');
    }
  });

  // Clean up IP connection counts (remove entries with 0 connections)
  ipConnectionCount.forEach((count, ip) => {
    if (count <= 0) {
      ipConnectionCount.delete(ip);
    }
  });
}, 10000);

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('Shutting down server...');
  
  // Mark all active tags as inactive in Firebase
  try {
    const snapshot = await tagsCollection.where('active', '==', true).get();
    const batch = db.batch();
    
    snapshot.forEach(doc => {
      batch.update(doc.ref, { 
        active: false,
        serverShutdown: true,
        removedAt: admin.firestore.FieldValue.serverTimestamp()
      });
    });
    
    await batch.commit();
    console.log('Updated Firebase tags for shutdown');
  } catch (error) {
    console.error('Error during shutdown:', error);
  }

  // Close all WebSocket connections
  wss.clients.forEach(client => {
    client.close(1001, 'Server shutting down');
  });

  server.close(() => {
    console.log('Server closed');
    process.exit(0);
  });
});

// Start server
const PORT = process.env.PORT || 8080;
server.listen(PORT, () => {
  console.log(`AR Tag Server running on port ${PORT}`);
  console.log(`Environment: ${process.env.NODE_ENV || 'development'}`);
  console.log(`Firebase Project: ${process.env.FIREBASE_PROJECT_ID}`);
});

//changed name
