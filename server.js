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
const cubesCollection = db.collection('cubes');
const questionsCollection = db.collection('questions');
const threadsCollection = db.collection('threads');

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

// Load threads from Firebase on startup
async function loadThreadsFromFirebase() {
  try {
    const snapshot = await threadsCollection.get();
    snapshot.forEach(doc => {
      threadsCache.set(doc.id, doc.data());
    });
    console.log(`Loaded ${threadsCache.size} threads from Firebase`);
  } catch (error) {
    console.error('Error loading threads from Firebase:', error);
  }
}

// In-memory storage for active connections
const activeConnections = new Map(); // socket -> { tag, connectionId, lastSeen, ip, token, ws, role }
const ipConnectionCount = new Map(); // ip -> count (for rate limiting)
const cubesCache = new Map();     // cubeId -> cube data
const questionsCache = new Map();  // questionId -> question data
const threadsCache = new Map();   // threadId -> thread data

// Rate limiting config
const RATE_LIMIT_WINDOW = parseInt(process.env.RATE_LIMIT_WINDOW) || 60000;
const MAX_CONNECTIONS_PER_IP = parseInt(process.env.MAX_CONNECTIONS_PER_IP) || 5;

// ─────────────────────────────────────────────────────────────────────────────
// PERMISSION HELPERS
// ─────────────────────────────────────────────────────────────────────────────

function canSeePrivate(connection) {
  return connection.role === 'admin' || connection.role === 'staff' || connection.role === 'peerMentor';
}

function canResolve(connection) {
  return connection.role === 'admin' || connection.role === 'staff' || connection.role === 'peerMentor';
}

function canBroadcast(connection) {
  return connection.role === 'admin' || connection.role === 'peerMentor';
}

// ─────────────────────────────────────────────────────────────────────────────
// QUESTION HANDLERS
// ─────────────────────────────────────────────────────────────────────────────

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
    createdAtMs: Date.now(),
    isBroadcast: false
  };

  try {
    await questionsCollection.doc(questionId).set(questionData);

    const cached = { ...questionData, createdAt: Date.now() / 1000 };
    questionsCache.set(questionId, cached);

    console.log(`[Question] ${isPrivate ? 'PRIVATE' : 'public'} question ${questionId} from ${connection.tag}`);

    ws.send(JSON.stringify({
      type: 'question_received',
      success: true,
      questionId
    }));

    broadcastQuestion(cached, connection.connectionId);

  } catch (error) {
    console.error('Error storing question:', error);
    ws.send(JSON.stringify({ type: 'error', message: 'Server error storing question' }));
  }
}

function broadcastQuestion(questionData, senderConnectionId) {
  const isPrivate = questionData.isPrivate;

  activeConnections.forEach((conn, client) => {
    if (client.readyState !== WebSocket.OPEN) return;
    if (conn.connectionId === senderConnectionId) return;

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

// ─────────────────────────────────────────────────────────────────────────────
// BROADCAST QUESTION HANDLER (peer mentors & admins)
// ─────────────────────────────────────────────────────────────────────────────

async function handleBroadcastQuestion(ws, message, connection) {
  if (!canBroadcast(connection)) {
    ws.send(JSON.stringify({ type: 'error', message: 'Only peer mentors or admins can broadcast' }));
    return;
  }

  if (!connection.tag) {
    ws.send(JSON.stringify({ type: 'error', message: 'Must be registered' }));
    return;
  }

  const text = (message.text || '').trim();
  if (!text || text.length > 1000) {
    ws.send(JSON.stringify({ type: 'error', message: 'Question text required (max 1000 chars)' }));
    return;
  }

  const questionId = message.questionId || ('b_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6));

  const questionData = {
    questionId,
    askedBy:    connection.tag,
    targetUser: null,
    text,
    isPrivate: false,
    isResolved: false,
    resolvedBy: null,
    resolvedAt: null,
    createdAt:  admin.firestore.FieldValue.serverTimestamp(),
    createdAtMs: Date.now(),
    isBroadcast: true
  };

  try {
    await questionsCollection.doc(questionId).set(questionData);
    const cached = { ...questionData, createdAt: Date.now() / 1000 };
    questionsCache.set(questionId, cached);

    ws.send(JSON.stringify({ type: 'broadcast_sent', success: true, questionId }));

    // Send to all connected clients except sender
    const payload = JSON.stringify({
      type: 'broadcast_question',
      questionId: cached.questionId,
      askedBy: cached.askedBy,
      text: cached.text,
      isBroadcast: true,
      createdAt: cached.createdAt
    });

    activeConnections.forEach((conn, client) => {
      if (client.readyState === WebSocket.OPEN && conn.connectionId !== connection.connectionId) {
        client.send(payload);
      }
    });

  } catch (error) {
    console.error('Error storing broadcast question:', error);
    ws.send(JSON.stringify({ type: 'error', message: 'Server error' }));
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// ANSWER BROADCAST
// ─────────────────────────────────────────────────────────────────────────────

async function handleAnswerBroadcast(ws, message, connection) {
  if (!connection.tag) return;

  const { questionId, hasAnswered } = message;
  if (!questionId || hasAnswered === undefined) return;

  const questionRef = questionsCollection.doc(questionId);
  const answerRef = questionRef.collection('answers').doc(connection.tag);

  try {
    await answerRef.set({
      tag: connection.tag,
      hasAnswered,
      timestamp: admin.firestore.FieldValue.serverTimestamp()
    });

    // Collect all answers for this broadcast and broadcast the map
    const answersSnapshot = await questionRef.collection('answers').get();
    const answers = {};
    answersSnapshot.forEach(doc => {
      answers[doc.id] = doc.data().hasAnswered;
    });

    const payload = JSON.stringify({
      type: 'broadcast_answers',
      questionId,
      answers
    });

    activeConnections.forEach((conn, client) => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(payload);
      }
    });
  } catch (error) {
    console.error('Error storing answer:', error);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// RESOLVE QUESTION
// ─────────────────────────────────────────────────────────────────────────────

async function handleResolveQuestion(ws, message, connection) {
  if (!connection.tag) {
    ws.send(JSON.stringify({ type: 'error', message: 'Must be registered to resolve questions' }));
    return;
  }

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
    await questionsCollection.doc(questionId).update({
      isResolved: true,
      resolvedBy: resolverTag,
      resolvedAt: admin.firestore.FieldValue.serverTimestamp()
    });

    if (questionsCache.has(questionId)) {
      const q = questionsCache.get(questionId);
      q.isResolved = true;
      q.resolvedBy = resolverTag;
      q.resolvedAt = Date.now() / 1000;
    } else {
      questionsCache.delete(questionId);
    }

    ws.send(JSON.stringify({
      type: 'question_resolved',
      questionId,
      resolvedBy: resolverTag,
      timestamp: Date.now()
    }));

    broadcastQuestionResolved(questionId, resolverTag, connection.connectionId);

  } catch (error) {
    console.error('Error resolving question:', error);
    ws.send(JSON.stringify({ type: 'error', message: 'Server error resolving question' }));
  }
}

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

// ─────────────────────────────────────────────────────────────────────────────
// SEND QUESTION LIST (updated to include isBroadcast flag)
// ─────────────────────────────────────────────────────────────────────────────

async function sendQuestionList(ws, connection) {
  try {
    const questions = [];

    questionsCache.forEach(q => {
      if (q.isResolved) return;

      if (q.isPrivate) {
        if (!canSeePrivate(connection)) {
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
        createdAt:   q.createdAt,
        isBroadcast: q.isBroadcast || false
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

// ─────────────────────────────────────────────────────────────────────────────
// DISCUSSION THREADS
// ─────────────────────────────────────────────────────────────────────────────

async function handleCreateThread(ws, message, connection) {
  if (!connection.tag) return;

  const { questionId } = message;
  if (!questionId) {
    ws.send(JSON.stringify({ type: 'error', message: 'questionId required' }));
    return;
  }

  // Find the question
  const question = questionsCache.get(questionId);
  if (!question) {
    ws.send(JSON.stringify({ type: 'error', message: 'Question not found' }));
    return;
  }

  const threadId = 'thread_' + questionId;
  if (threadsCache.has(threadId)) {
    // Thread already exists, just send it
    ws.send(JSON.stringify({ type: 'thread_created', ...threadsCache.get(threadId) }));
    return;
  }

  const threadData = {
    id: threadId,
    questionId,
    title: `Question #${questionId.slice(-4)}`,
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
    createdAtMs: Date.now(),
    isResolved: false,
    resolvedBy: null,
    resolvedAt: null,
    messages: [] // will store array of { id, author, text, timestamp }
  };

  try {
    await threadsCollection.doc(threadId).set(threadData);
    threadsCache.set(threadId, threadData);

    // Update question with threadId
    await questionsCollection.doc(questionId).update({ threadId: threadId });
    question.threadId = threadId; // update cache

    const payload = JSON.stringify({ type: 'thread_created', ...threadData });
    ws.send(payload);

    // Broadcast to all
    activeConnections.forEach((conn, client) => {
      if (client.readyState === WebSocket.OPEN && conn.connectionId !== connection.connectionId) {
        client.send(payload);
      }
    });

  } catch (error) {
    console.error('Error creating thread:', error);
    ws.send(JSON.stringify({ type: 'error', message: 'Server error' }));
  }
}

async function handleThreadMessage(ws, message, connection) {
  if (!connection.tag) return;

  const { threadId, text } = message;
  if (!threadId || !text) return;

  const threadRef = threadsCollection.doc(threadId);
  const thread = threadsCache.get(threadId);

  if (!thread) {
    ws.send(JSON.stringify({ type: 'error', message: 'Thread not found' }));
    return;
  }
  if (thread.isResolved) {
    ws.send(JSON.stringify({ type: 'error', message: 'Thread is resolved' }));
    return;
  }

  const msgId = 'tm_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6);
  const threadMessage = {
    id: msgId,
    author: connection.tag,
    text: text.trim(),
    timestamp: admin.firestore.FieldValue.serverTimestamp(),
    timestampMs: Date.now()
  };

  try {
    await threadRef.update({
      messages: admin.firestore.FieldValue.arrayUnion(threadMessage)
    });

    // Update cache
    if (threadsCache.has(threadId)) {
      threadsCache.get(threadId).messages.push(threadMessage);
    }

    ws.send(JSON.stringify({ type: 'thread_message_sent', threadId, message: threadMessage }));

    const payload = JSON.stringify({ type: 'thread_message_added', threadId, message: threadMessage });
    activeConnections.forEach((conn, client) => {
      if (client.readyState === WebSocket.OPEN && conn.connectionId !== connection.connectionId) {
        client.send(payload);
      }
    });

  } catch (error) {
    console.error('Error adding thread message:', error);
  }
}

async function handleResolveThread(ws, message, connection) {
  if (!connection.tag) return;

  const { threadId, resolvedBy } = message;
  if (!threadId) return;

  const thread = threadsCache.get(threadId);
  if (!thread) return;

  // Permission: thread creator (first message author) or privileged role
  const isCreator = thread.messages.length > 0 && thread.messages[0].author === connection.tag;
  if (!isCreator && !canResolve(connection)) {
    ws.send(JSON.stringify({ type: 'error', message: 'Not authorized to resolve this thread' }));
    return;
  }

  try {
    await threadsCollection.doc(threadId).update({
      isResolved: true,
      resolvedBy: resolvedBy || connection.tag,
      resolvedAt: admin.firestore.FieldValue.serverTimestamp()
    });

    if (threadsCache.has(threadId)) {
      const t = threadsCache.get(threadId);
      t.isResolved = true;
      t.resolvedBy = resolvedBy || connection.tag;
    }

    const payload = JSON.stringify({ type: 'thread_resolved', threadId, resolvedBy: resolvedBy || connection.tag });
    ws.send(payload);
    activeConnections.forEach((conn, client) => {
      if (client.readyState === WebSocket.OPEN && conn.connectionId !== connection.connectionId) {
        client.send(payload);
      }
    });

  } catch (error) {
    console.error('Error resolving thread:', error);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// UPDATE ROLE (existing handler)
// ─────────────────────────────────────────────────────────────────────────────

async function handleUpdateRole(ws, message, connection) {
  if (!connection.tag) {
    ws.send(JSON.stringify({ type: 'error', message: 'Must be registered to change roles' }));
    return;
  }

  const targetTag = message.targetTag || connection.tag;
  const newRole  = message.role;

  const validRoles = ['student', 'peerMentor', 'staff', 'admin'];
  if (!validRoles.includes(newRole)) {
    ws.send(JSON.stringify({ type: 'error', message: `Invalid role: ${newRole}` }));
    return;
  }

  try {
    if (targetTag === connection.tag) {
      const currentRole = connection.role;
      if (currentRole === 'student' && newRole === 'peerMentor') {
        // allow
      } else if (currentRole === 'peerMentor' && newRole === 'student') {
        // allow
      } else {
        ws.send(JSON.stringify({ type: 'error', message: 'You can only toggle between student and peer mentor' }));
        return;
      }

      connection.role = newRole;
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

    // Admin/staff changing another user's role
    if (connection.role !== 'admin' && connection.role !== 'staff') {
      ws.send(JSON.stringify({ type: 'error', message: 'Only staff or admin can change other users\' roles' }));
      return;
    }

    const targetDoc = await tagsCollection.doc(targetTag).get();
    if (!targetDoc.exists) {
      ws.send(JSON.stringify({ type: 'error', message: 'Target user not found' }));
      return;
    }

    await tagsCollection.doc(targetTag).update({ role: newRole });

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

// ─────────────────────────────────────────────────────────────────────────────
// CONNECTION HANDLING
// ─────────────────────────────────────────────────────────────────────────────

wss.on('connection', async (ws, req) => {
  const ip = req.socket.remoteAddress;
  console.log(`New connection from ${ip}`);

  const currentConnections = ipConnectionCount.get(ip) || 0;
  if (currentConnections >= MAX_CONNECTIONS_PER_IP) {
    console.log(`Rate limit exceeded for ${ip}`);
    ws.close(1008, 'Too many connections from this IP');
    return;
  }

  ipConnectionCount.set(ip, currentConnections + 1);

  const connectionId = generateConnectionId();
  const token = jwt.sign(
    { connectionId, ip, iat: Date.now() },
    JWT_SECRET,
    { expiresIn: TOKEN_EXPIRY }
  );

  activeConnections.set(ws, {
    tag: null,
    ws: ws,
    connectionId,
    ip,
    token,
    lastSeen: Date.now(),
    connectedAt: Date.now(),
    role: 'student'
  });

  ws.send(JSON.stringify({
    type: 'connected',
    connectionId,
    token,
    message: 'Connected to Flock Server',
    timestamp: new Date().toISOString()
  }));

  connectionsCollection.doc(connectionId).set({
    ip,
    connectedAt: admin.firestore.FieldValue.serverTimestamp(),
    userAgent: req.headers['user-agent'] || 'unknown'
  }).catch(console.error);

  ws.on('message', async (data) => {
    try {
      const message = JSON.parse(data.toString());
      console.log(`[${connectionId}] Received:`, message.type);

      const connection = activeConnections.get(ws);
      if (connection) connection.lastSeen = Date.now();

      if (message.token && connection && message.token !== connection.token) {
        ws.send(JSON.stringify({ type: 'error', message: 'Invalid token' }));
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
        case 'broadcast_question':
          await handleBroadcastQuestion(ws, message, connection);
          break;
        case 'answer_broadcast':
          await handleAnswerBroadcast(ws, message, connection);
          break;
        case 'resolve_question':
          await handleResolveQuestion(ws, message, connection);
          break;
        case 'get_questions':
          await sendQuestionList(ws, connection);
          break;
        case 'create_thread':
          await handleCreateThread(ws, message, connection);
          break;
        case 'thread_message':
          await handleThreadMessage(ws, message, connection);
          break;
        case 'resolve_thread':
          await handleResolveThread(ws, message, connection);
          break;
        case 'update_role':
          await handleUpdateRole(ws, message, connection);
          break;
        case 'ping':
          ws.send(JSON.stringify({ type: 'pong', timestamp: Date.now() }));
          break;
        default:
          ws.send(JSON.stringify({ type: 'error', message: 'Unknown command' }));
      }
    } catch (error) {
      console.error('Error processing message:', error);
      ws.send(JSON.stringify({ type: 'error', message: 'Invalid message format' }));
    }
  });

  ws.on('close', async () => {
    console.log(`Connection closed from ${ip}`);

    const count = ipConnectionCount.get(ip) || 1;
    ipConnectionCount.set(ip, Math.max(0, count - 1));

    const connection = activeConnections.get(ws);
    if (connection) {
      if (connection.tag) await removeTag(connection.tag, connection.connectionId);

      await connectionsCollection.doc(connection.connectionId).update({
        disconnectedAt: admin.firestore.FieldValue.serverTimestamp()
      }).catch(console.error);

      activeConnections.delete(ws);
    }
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// TAG & MESSAGE HANDLERS (existing, modified for reply/thread fields)
// ─────────────────────────────────────────────────────────────────────────────

async function handleTagRegistration(ws, requestedTag, requestedRole) {
  const connection = activeConnections.get(ws);
  if (!connection) {
    ws.send(JSON.stringify({ type: 'register_response', success: false, message: 'Connection not found' }));
    return;
  }

  if (!requestedTag || typeof requestedTag !== 'string') {
    ws.send(JSON.stringify({ type: 'register_response', success: false, message: 'Invalid tag format' }));
    return;
  }

  const sanitizedTag = requestedTag.replace(/[^a-zA-Z0-9_]/g, '');
  if (sanitizedTag.length < 2 || sanitizedTag.length > 20) {
    ws.send(JSON.stringify({ type: 'register_response', success: false, message: 'Tag must be 2-20 characters' }));
    return;
  }

  try {
    const tagDoc = await tagsCollection.doc(sanitizedTag).get();
    if (tagDoc.exists && tagDoc.data().active) {
      ws.send(JSON.stringify({ type: 'register_response', success: false, message: 'Tag already in use' }));
      return;
    }

    if (connection.tag) await removeTag(connection.tag, connection.connectionId);

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

    connection.tag = sanitizedTag;
    connection.role = validRoles.includes(requestedRole) ? requestedRole : 'student';

    ws.send(JSON.stringify({ type: 'register_response', success: true, tag: sanitizedTag }));
    broadcastActiveTags().catch(console.error);
  } catch (error) {
    console.error('Registration error:', error);
    ws.send(JSON.stringify({ type: 'register_response', success: false, message: 'Server error' }));
  }
}

async function handleTagUnregistration(ws) {
  const connection = activeConnections.get(ws);
  if (connection && connection.tag) {
    const tag = connection.tag;
    connection.tag = null;
    ws.send(JSON.stringify({ type: 'unregister_response', success: true, message: 'Tag unregistered' }));
    removeTag(tag, connection.connectionId).catch(console.error);
  }
}

async function removeTag(tag, connectionId) {
  try {
    await tagsCollection.doc(tag).update({
      active: false,
      removedAt: admin.firestore.FieldValue.serverTimestamp()
    });
    console.log(`Tag removed: ${tag} (${connectionId})`);
    broadcastActiveTags().catch(console.error);
  } catch (error) {
    console.error('Error removing tag:', error);
  }
}

async function sendActiveTags(ws) {
  try {
    const snapshot = await tagsCollection.where('active', '==', true).get();
    const tags = [];
    snapshot.forEach(doc => tags.push(doc.data().tag));

    ws.send(JSON.stringify({ type: 'tag_list', tags, count: tags.length, timestamp: Date.now() }));
  } catch (error) {
    console.error('Error fetching tags:', error);
    ws.send(JSON.stringify({ type: 'error', message: 'Error fetching tags' }));
  }
}

async function broadcastActiveTags() {
  try {
    const snapshot = await tagsCollection.where('active', '==', true).get();
    const tags = [];
    snapshot.forEach(doc => tags.push(doc.data().tag));

    const message = JSON.stringify({ type: 'tag_list', tags, count: tags.length, timestamp: Date.now() });
    activeConnections.forEach((conn, client) => {
      if (client.readyState === WebSocket.OPEN) client.send(message);
    });
    console.log(`Broadcasted ${tags.length} active tags`);
  } catch (error) {
    console.error('Error broadcasting tags:', error);
  }
}

async function handleMessage(ws, message, connection) {
  if (!message.text || typeof message.text !== 'string') {
    ws.send(JSON.stringify({ type: 'message_response', success: false, message: 'Message text required' }));
    return;
  }

  const trimmed = message.text.trim();
  if (trimmed.length === 0 || trimmed.length > 1000) {
    ws.send(JSON.stringify({ type: 'message_response', success: false, message: 'Invalid message length' }));
    return;
  }

  const messageData = {
    text: trimmed,
    connectionId: connection.connectionId,
    tag: connection.tag || 'anonymous',
    ip: connection.ip,
    sentAt: admin.firestore.FieldValue.serverTimestamp(),
    timestamp: Date.now(),
    replyToMessageId: message.replyToMessageId || null,
    threadId: message.threadId || null
  };

  try {
    const docRef = await messagesCollection.add(messageData);
    ws.send(JSON.stringify({ type: 'message_response', success: true, messageId: docRef.id }));

    const broadcastPayload = JSON.stringify({
      type: 'new_message',
      messageId: docRef.id,
      text: messageData.text,
      from: messageData.tag,
      connectionId: messageData.connectionId,
      timestamp: messageData.timestamp,
      replyToMessageId: messageData.replyToMessageId,
      threadId: messageData.threadId
    });

    activeConnections.forEach((conn, client) => {
      if (client.readyState === WebSocket.OPEN && conn.connectionId !== connection.connectionId) {
        client.send(broadcastPayload);
      }
    });
  } catch (error) {
    console.error('Error storing message:', error);
    ws.send(JSON.stringify({ type: 'message_response', success: false, message: 'Server error' }));
  }
}

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
        connectionId: data.connectionId,
        replyToMessageId: data.replyToMessageId,
        threadId: data.threadId
      });
    });

    ws.send(JSON.stringify({ type: 'message_history', messages, count: messages.length, timestamp: Date.now() }));
  } catch (error) {
    console.error('Error fetching messages:', error);
    ws.send(JSON.stringify({ type: 'error', message: 'Error fetching message history' }));
  }
}

// Cube handlers (unchanged except for using existing code)
async function handleCubeLocation(ws, message, connection) {
  if (!message.position || !message.rotation || !message.cubeId || !message.ownerTag) {
    ws.send(JSON.stringify({ type: 'cube_location_response', success: false, message: 'Position, rotation, cubeId, and ownerTag required' }));
    return;
  }

  try {
    const cubeData = {
      cubeId: message.cubeId,
      position: { x: parseFloat(message.position.x), y: parseFloat(message.position.y), z: parseFloat(message.position.z) },
      rotation: {
        x: parseFloat(message.rotation.x), y: parseFloat(message.rotation.y), z: parseFloat(message.rotation.z),
        w: message.rotation.w !== undefined ? parseFloat(message.rotation.w) : null
      },
      ownerTag: message.ownerTag,
      connectionId: connection.connectionId,
      ip: connection.ip,
      createdAt: message.createdAt ? new Date(message.createdAt * 1000) : admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    };

    await cubesCollection.doc(message.cubeId).set(cubeData, { merge: true });
    cubesCache.set(message.cubeId, cubeData);

    ws.send(JSON.stringify({ type: 'cube_location_response', success: true, cubeId: message.cubeId, timestamp: Date.now() }));

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

    activeConnections.forEach((conn, client) => {
      if (client.readyState === WebSocket.OPEN && conn.connectionId !== connection.connectionId) {
        client.send(broadcastPayload);
      }
    });
  } catch (error) {
    console.error('Error storing cube:', error);
    ws.send(JSON.stringify({ type: 'cube_location_response', success: false, message: 'Server error' }));
  }
}

async function sendCubeList(ws) {
  try {
    const cubes = Array.from(cubesCache.values());
    ws.send(JSON.stringify({ type: 'cube_list', cubes, count: cubes.length, timestamp: Date.now() }));
  } catch (error) {
    console.error('Error sending cube list:', error);
    ws.send(JSON.stringify({ type: 'error', message: 'Error fetching cube list' }));
  }
}

// Utility functions (unchanged)
function generateConnectionId() {
  return 'conn_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
}

function verifyToken(token) {
  try {
    return jwt.verify(token, JWT_SECRET);
  } catch (error) {
    return null;
  }
}

setInterval(() => {
  const now = Date.now();
  const timeout = 30000;
  activeConnections.forEach((connection, ws) => {
    if (now - connection.lastSeen > timeout && ws.readyState === WebSocket.OPEN) {
      console.log(`Closing stale connection: ${connection.connectionId}`);
      ws.close(1000, 'Connection timeout');
    }
  });
  ipConnectionCount.forEach((count, ip) => {
    if (count <= 0) ipConnectionCount.delete(ip);
  });
}, 10000);

process.on('SIGINT', async () => {
  console.log('Shutting down server...');
  try {
    const snapshot = await tagsCollection.where('active', '==', true).get();
    const batch = db.batch();
    snapshot.forEach(doc => {
      batch.update(doc.ref, { active: false, serverShutdown: true, removedAt: admin.firestore.FieldValue.serverTimestamp() });
    });
    await batch.commit();
  } catch (error) {
    console.error('Error during shutdown:', error);
  }
  wss.clients.forEach(client => client.close(1001, 'Server shutting down'));
  server.close(() => {
    console.log('Server closed');
    process.exit(0);
  });
});

const PORT = process.env.PORT || 8080;

Promise.all([loadCubesFromFirebase(), loadQuestionsFromFirebase(), loadThreadsFromFirebase()]).then(() => {
  server.listen(PORT, () => {
    console.log(`Flock Server running on port ${PORT}`);
    console.log(`Environment: ${process.env.NODE_ENV || 'development'}`);
    console.log(`Firebase Project: ${process.env.FIREBASE_PROJECT_ID}`);
    console.log(`Cubes cache initialized with ${cubesCache.size} cubes`);
  });
}).catch(error => {
  console.error('Failed to load data on startup:', error);
  server.listen(PORT, () => {
    console.log(`Flock Server running on port ${PORT} (initial load failed)`);
  });
});