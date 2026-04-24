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
const cubesCollection = db.collection('cubes'); // Renamed from cube_locations for clarity
const questionsCollection = db.collection('questions');

// Create HTTP server
const server = http.createServer();
const wss = new WebSocket.Server({ server });

// Load cubes from Firebase on startup
async function loadCubesFromFirebase() {
  try {
    const snapshot = await cubesCollection.get();
    snapshot.forEach(doc => {
      cubesCache.set(doc.id, doc.data());
    });
    console.log(`Loaded ${cubesCache.size} cubes from Firebase`);
  } catch (error) {
    console.error('Error loading cubes from Firebase:', error);
  }
}


// Load questions from Firebase on startup
async function loadQuestionsFromFirebase() {
  try {
    const snapshot = await questionsCollection
      .where('isResolved', '==', false)
      .get();
    snapshot.forEach(doc => {
      questionsCache.set(doc.id, doc.data());
    });
    console.log(`Loaded ${questionsCache.size} open question(s) from Firebase`);
  } catch (error) {
    console.error('Error loading questions from Firebase:', error);
  }
}

// In-memory storage for active connections
const activeConnections = new Map(); // socket -> { tag, connectionId, lastSeen, ip, token, ws }
const ipConnectionCount = new Map(); // ip -> count (for rate limiting)
const cubesCache = new Map();     // cubeId -> cube data (persists across client connections)
const questionsCache = new Map();  // questionId -> question data (persists across client connections)

// Rate limiting config
const RATE_LIMIT_WINDOW = parseInt(process.env.RATE_LIMIT_WINDOW) || 60000;
const MAX_CONNECTIONS_PER_IP = parseInt(process.env.MAX_CONNECTIONS_PER_IP) || 5;

// ─────────────────────────────────────────────────────────────────────────────
// QUESTION HANDLERS
// ─────────────────────────────────────────────────────────────────────────────

// Helper: does this connection's role allow seeing private questions?
function canSeePrivate(connection) {
  return connection.role === 'admin' || connection.role === 'staff' || connection.role === 'peerMentor';
}

// Helper: does this connection's role allow resolving questions?
function canResolve(connection) {
  return connection.role === 'admin' || connection.role === 'staff' || connection.role === 'peerMentor';
}

// Handle send_question / send_private_question
async function handleQuestion(ws, message, connection) {
  if (!connection.tag) {
    ws.send(JSON.stringify({ type: 'error', message: 'Must be registered to ask a question' }));
    return;
  }

  const text = (message.text || '').trim();
  if (!text || text.length > 1000) {
    ws.send(JSON.stringify({ type: 'error', message: 'Question text required (max 1000 chars)' }));
    return;
  }

  const isPrivate = message.type === 'send_private_question' || message.isPrivate === true;
  const questionId = message.questionId || ('q_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6));

  const questionData = {
    questionId,
    askedBy:    connection.tag,
    targetUser: message.targetUser || null,
    text,
    isPrivate,
    isResolved: false,
    resolvedBy: null,
    resolvedAt: null,
    createdAt:  admin.firestore.FieldValue.serverTimestamp(),
    createdAtMs: Date.now()
  };

  try {
    // Persist to Firestore
    await questionsCollection.doc(questionId).set(questionData);

    // Store a serialisable copy in the in-memory cache
    const cached = { ...questionData, createdAt: Date.now() / 1000 };
    questionsCache.set(questionId, cached);

    console.log(`[Question] ${isPrivate ? 'PRIVATE' : 'public'} question ${questionId} from ${connection.tag}`);

    // Confirm to sender
    ws.send(JSON.stringify({
      type: 'question_received',
      success: true,
      questionId
    }));

    // Broadcast to eligible connections
    broadcastQuestion(cached, connection.connectionId);

  } catch (error) {
    console.error('Error storing question:', error);
    ws.send(JSON.stringify({ type: 'error', message: 'Server error storing question' }));
  }
}

// Broadcast a new question to all eligible connections
function broadcastQuestion(questionData, senderConnectionId) {
  const isPrivate = questionData.isPrivate;

  activeConnections.forEach((conn, client) => {
    if (client.readyState !== WebSocket.OPEN) return;
    if (conn.connectionId === senderConnectionId) return; // sender already sees it locally

    // Private question routing:
    //   • admins, staff and peer mentors always receive it
    //   • the targeted user (if any) receives it
    //   • everyone else does NOT
    if (isPrivate) {
      const isPrivileged = canSeePrivate(conn);
      const isTarget     = conn.tag && questionData.targetUser && conn.tag === questionData.targetUser;
      if (!isPrivileged && !isTarget) return;
    }

    client.send(JSON.stringify({
      type: isPrivate ? 'new_private_question' : 'new_question',
      questionId:  questionData.questionId,
      askedBy:     questionData.askedBy,
      targetUser:  questionData.targetUser,
      text:        questionData.text,
      isPrivate:   questionData.isPrivate,
      isResolved:  false,
      createdAt:   questionData.createdAt
    }));
  });
}

// Handle resolve_question
async function handleResolveQuestion(ws, message, connection) {
  if (!connection.tag) {
    ws.send(JSON.stringify({ type: 'error', message: 'Must be registered to resolve questions' }));
    return;
  }

  // Role gate — only staff, admin, and peer mentors may resolve
  if (!canResolve(connection)) {
    ws.send(JSON.stringify({ type: 'error', message: 'Only staff, admin, or peer mentors can resolve questions' }));
    return;
  }

  const { questionId, resolvedBy } = message;
  if (!questionId) {
    ws.send(JSON.stringify({ type: 'error', message: 'questionId required' }));
    return;
  }

  const resolverTag = resolvedBy || connection.tag;

  try {
    // Update Firestore
    await questionsCollection.doc(questionId).update({
      isResolved: true,
      resolvedBy: resolverTag,
      resolvedAt: admin.firestore.FieldValue.serverTimestamp()
    });

    // Update cache
    if (questionsCache.has(questionId)) {
      const q = questionsCache.get(questionId);
      q.isResolved = true;
      q.resolvedBy = resolverTag;
      q.resolvedAt = Date.now() / 1000;
    } else {
      // Remove from cache if it wasn't there (already resolved race condition)
      questionsCache.delete(questionId);
    }

    console.log(`[Question] ${questionId} resolved by ${resolverTag}`);

    // Confirm to resolver
    ws.send(JSON.stringify({
      type: 'question_resolved',
      questionId,
      resolvedBy: resolverTag,
      timestamp: Date.now()
    }));

    // Broadcast resolution to everyone (public event — safe to send to all)
    broadcastQuestionResolved(questionId, resolverTag, connection.connectionId);

  } catch (error) {
    console.error('Error resolving question:', error);
    ws.send(JSON.stringify({ type: 'error', message: 'Server error resolving question' }));
  }
}

// Broadcast a question_resolved event to all connected clients
function broadcastQuestionResolved(questionId, resolvedBy, senderConnectionId) {
  const payload = JSON.stringify({
    type: 'question_resolved',
    questionId,
    resolvedBy,
    timestamp: Date.now()
  });

  activeConnections.forEach((conn, client) => {
    if (client.readyState === WebSocket.OPEN && conn.connectionId !== senderConnectionId) {
      client.send(payload);
    }
  });
}

// Send the question list to a newly-joined client (role-filtered)
async function sendQuestionList(ws, connection) {
  try {
    // Send only open questions from in-memory cache — fastest path
    const questions = [];

    questionsCache.forEach(q => {
      if (q.isResolved) return;

      // Apply the same private-visibility rules as the client
      if (q.isPrivate) {
        if (!canSeePrivate(connection)) {
          // Student/peer mentor: only see their own private questions (as asker or target)
          const isAsker  = connection.tag && q.askedBy    === connection.tag;
          const isTarget = connection.tag && q.targetUser === connection.tag;
          if (!isAsker && !isTarget) return;
        }
      }

      questions.push({
        questionId:  q.questionId,
        askedBy:     q.askedBy,
        targetUser:  q.targetUser,
        text:        q.text,
        isPrivate:   q.isPrivate,
        isResolved:  q.isResolved,
        resolvedBy:  q.resolvedBy,
        createdAt:   q.createdAt
      });
    });

    ws.send(JSON.stringify({
      type: 'question_list',
      questions,
      count: questions.length,
      timestamp: Date.now()
    }));

    console.log(`[Questions] Sent ${questions.length} visible question(s) to ${connection.tag || 'unregistered'} (${connection.role})`);

  } catch (error) {
    console.error('Error sending question list:', error);
    ws.send(JSON.stringify({ type: 'error', message: 'Error fetching questions' }));
  }
}

// Handle update_role (self-promotion or admin/staff setting another user's role)
async function handleUpdateRole(ws, message, connection) {
  if (!connection.tag) {
    ws.send(JSON.stringify({ type: 'error', message: 'Must be registered to change roles' }));
    return;
  }

  const targetTag = message.targetTag || connection.tag;   // if not specified, update self
  const newRole  = message.role;

  // Validate new role
  const validRoles = ['student', 'peerMentor', 'staff', 'admin'];
  if (!validRoles.includes(newRole)) {
    ws.send(JSON.stringify({ type: 'error', message: `Invalid role: ${newRole}` }));
    return;
  }

  try {
    // ----- Case 1: User updates their own role (self-promotion) -----
    if (targetTag === connection.tag) {
      // For now, only allow student <-> peerMentor transition
      const currentRole = connection.role;
      if (currentRole === 'student' && newRole === 'peerMentor') {
        // Allow
      } else if (currentRole === 'peerMentor' && newRole === 'student') {
        // Optional: allow demotion back to student
      } else {
        ws.send(JSON.stringify({
          type: 'error',
          message: 'You can only toggle between student and peer mentor'
        }));
        return;
      }

      // Update in-memory connection
      connection.role = newRole;

      // Update Firebase
      await tagsCollection.doc(connection.tag).update({ role: newRole });

      ws.send(JSON.stringify({
        type: 'role_updated',
        success: true,
        tag: connection.tag,
        role: newRole,
        message: `Your role is now ${newRole}`
      }));

      console.log(`[Role] ${connection.tag} self-promoted to ${newRole}`);
      return;
    }

    // ----- Case 2: Admin/staff changes another user's role -----
    if (connection.role !== 'admin' && connection.role !== 'staff') {
      ws.send(JSON.stringify({ type: 'error', message: 'Only staff or admin can change other users\' roles' }));
      return;
    }

    // Update the target's Firestore document
    const targetDoc = await tagsCollection.doc(targetTag).get();
    if (!targetDoc.exists) {
      ws.send(JSON.stringify({ type: 'error', message: 'Target user not found' }));
      return;
    }

    await tagsCollection.doc(targetTag).update({ role: newRole });

    // If the target user is currently connected, push the change to them
    let targetConnection = null;
    let targetWs = null;
    activeConnections.forEach((conn, client) => {
      if (conn.tag === targetTag && client.readyState === WebSocket.OPEN) {
        targetConnection = conn;
        targetWs = client;
      }
    });

    if (targetConnection) {
      targetConnection.role = newRole;
      // Send a role_changed event so the target's client updates its UI
      if (targetWs) {
        targetWs.send(JSON.stringify({
          type: 'role_changed',
          tag: targetTag,
          role: newRole,
          changedBy: connection.tag
        }));
      }
    }

    ws.send(JSON.stringify({
      type: 'role_set',
      success: true,
      targetTag,
      newRole,
      message: `${targetTag}'s role set to ${newRole}`
    }));

    console.log(`[Role] ${connection.tag} (${connection.role}) set ${targetTag}'s role to ${newRole}`);

  } catch (error) {
    console.error('Error updating role:', error);
    ws.send(JSON.stringify({ type: 'error', message: 'Server error updating role' }));
  }
}

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
  
  // Store connection — now includes ws reference for role updates
  activeConnections.set(ws, {
    tag: null,
    ws: ws,            // store WebSocket reference
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
    message: 'Connected to Flock Server',
    timestamp: new Date().toISOString()
  }));

  // Log connection to Firebase asynchronously
  connectionsCollection.doc(connectionId).set({
    ip,
    connectedAt: admin.firestore.FieldValue.serverTimestamp(),
    userAgent: req.headers['user-agent'] || 'unknown'
  }).catch(error => {
    console.error('Error logging connection:', error);
  });

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
      
      // Verify token if provided (optional)
      if (message.token && connection && message.token !== connection.token) {
        ws.send(JSON.stringify({
          type: 'error',
          message: 'Invalid token'
        }));
        return;
      }

      switch (message.type) {
        case 'register_tag':
          await handleTagRegistration(ws, message.tag, message.role);
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
        case 'cube_location':
          await handleCubeLocation(ws, message, connection);
          break;
        case 'get_cubes':
          await sendCubeList(ws);
          break;
        case 'send_question':
        case 'send_private_question':
          await handleQuestion(ws, message, connection);
          break;
        case 'resolve_question':
          await handleResolveQuestion(ws, message, connection);
          break;
        case 'get_questions':
          await sendQuestionList(ws, connection);
          break;
        case 'update_role':
          await handleUpdateRole(ws, message, connection);
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
async function handleTagRegistration(ws, requestedTag, requestedRole) {
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
    const validRoles = ['student', 'peerMentor', 'staff', 'admin'];
    await tagsCollection.doc(sanitizedTag).set({
      tag: sanitizedTag,
      connectionId: connection.connectionId,
      active: true,
      registeredAt: admin.firestore.FieldValue.serverTimestamp(),
      lastSeen: admin.firestore.FieldValue.serverTimestamp(),
      ip: connection.ip,
      role: validRoles.includes(requestedRole) ? requestedRole : 'student'
    });

    // Update connection with new tag and role
    connection.tag = sanitizedTag;
    connection.role = validRoles.includes(requestedRole) ? requestedRole : 'student';

    console.log(`Tag registered: ${sanitizedTag} (${connection.connectionId})`);

    // Send success response immediately
    ws.send(JSON.stringify({
      type: 'register_response',
      success: true,
      message: 'Tag registered successfully',
      tag: sanitizedTag
    }));

    // Broadcast updated tag list to all connections
    broadcastActiveTags().catch(error => {
      console.error('Error in broadcastActiveTags:', error);
    });

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
    const tag = connection.tag;
    connection.tag = null;
    
    // Send response immediately
    ws.send(JSON.stringify({
      type: 'unregister_response',
      success: true,
      message: 'Tag unregistered'
    }));
    
    // Remove tag asynchronously
    removeTag(tag, connection.connectionId).catch(error => {
      console.error('Error in removeTag:', error);
    });
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
    broadcastActiveTags().catch(error => {
      console.error('Error in broadcastActiveTags:', error);
    });
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

  // Limit message length
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

// Handle cube location updates
async function handleCubeLocation(ws, message, connection) {
  // Validate required fields
  if (!message.position || !message.rotation || !message.cubeId || !message.ownerTag) {
    ws.send(JSON.stringify({
      type: 'cube_location_response',
      success: false,
      message: 'Position, rotation, cubeId, and ownerTag required'
    }));
    return;
  }

  // Validate position
  if (typeof message.position !== 'object' || 
      message.position.x === undefined || 
      message.position.y === undefined || 
      message.position.z === undefined) {
    ws.send(JSON.stringify({
      type: 'cube_location_response',
      success: false,
      message: 'Invalid position format. Expected {x, y, z}'
    }));
    return;
  }

  // Validate rotation
  if (typeof message.rotation !== 'object' || 
      message.rotation.x === undefined || 
      message.rotation.y === undefined || 
      message.rotation.z === undefined) {
    ws.send(JSON.stringify({
      type: 'cube_location_response',
      success: false,
      message: 'Invalid rotation format. Expected {x, y, z} or {x, y, z, w}'
    }));
    return;
  }

  try {
    // Create cube object
    const cubeData = {
      cubeId: message.cubeId,
      position: {
        x: parseFloat(message.position.x),
        y: parseFloat(message.position.y),
        z: parseFloat(message.position.z)
      },
      rotation: {
        x: parseFloat(message.rotation.x),
        y: parseFloat(message.rotation.y),
        z: parseFloat(message.rotation.z),
        w: message.rotation.w !== undefined ? parseFloat(message.rotation.w) : null
      },
      ownerTag: message.ownerTag,
      connectionId: connection.connectionId,
      ip: connection.ip,
      createdAt: message.createdAt ? new Date(message.createdAt * 1000) : admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    };

    // Store cube in Firebase (use cubeId as document ID)
    await cubesCollection.doc(message.cubeId).set(cubeData, { merge: true });

    // Store in in-memory cache
    cubesCache.set(message.cubeId, cubeData);

    console.log(`Cube stored: ${message.cubeId} from ${connection.connectionId}`);

    // Send confirmation to sender
    ws.send(JSON.stringify({
      type: 'cube_location_response',
      success: true,
      message: 'Cube location updated',
      cubeId: message.cubeId,
      timestamp: Date.now()
    }));

    // Broadcast cube to all other connections
    await broadcastCube(cubeData, connection.connectionId);

  } catch (error) {
    console.error('Error storing cube:', error);
    ws.send(JSON.stringify({
      type: 'cube_location_response',
      success: false,
      message: 'Server error storing cube'
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
    if (client.readyState === WebSocket.OPEN && connection.connectionId !== senderConnectionId) {
      client.send(broadcastPayload);
    }
  });

  console.log(`Broadcasted message to ${activeConnections.size - 1} other connections`);
}

// Broadcast cube to all other connections
async function broadcastCube(cubeData, senderConnectionId) {
  const broadcastPayload = JSON.stringify({
    type: 'cube_location_update',
    cubeId: cubeData.cubeId,
    position: cubeData.position,
    rotation: cubeData.rotation,
    from: cubeData.ownerTag,
    ownerTag: cubeData.ownerTag,
    connectionId: cubeData.connectionId,
    createdAt: cubeData.createdAt instanceof Date ? cubeData.createdAt.getTime() / 1000 : Date.now() / 1000,
    timestamp: Date.now()
  });

  activeConnections.forEach((connection, client) => {
    if (client.readyState === WebSocket.OPEN && connection.connectionId !== senderConnectionId) {
      client.send(broadcastPayload);
    }
  });

  console.log(`Broadcasted cube to ${activeConnections.size - 1} other connections`);
}

// Send cube list to a client
async function sendCubeList(ws) {
  try {
    const cubes = Array.from(cubesCache.values());
    
    ws.send(JSON.stringify({
      type: 'cube_list',
      cubes: cubes,
      count: cubes.length,
      timestamp: Date.now()
    }));

    console.log(`Sent ${cubes.length} cubes to client`);
  } catch (error) {
    console.error('Error sending cube list:', error);
    ws.send(JSON.stringify({
      type: 'error',
      message: 'Error fetching cube list'
    }));
  }
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

  // Clean up IP connection counts
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

// Load cubes and questions from Firebase before starting the server
Promise.all([loadCubesFromFirebase(), loadQuestionsFromFirebase()]).then(() => {
  server.listen(PORT, () => {
    console.log(`Flock Server running on port ${PORT}`);
    console.log(`Environment: ${process.env.NODE_ENV || 'development'}`);
    console.log(`Firebase Project: ${process.env.FIREBASE_PROJECT_ID}`);
    console.log(`Cubes cache initialized with ${cubesCache.size} cubes`);
  });
}).catch(error => {
  console.error('Failed to load data on startup:', error);
  server.listen(PORT, () => {
    console.log(`Flock Server running on port ${PORT} (initial load failed, will try again on next request)`);
  });
});