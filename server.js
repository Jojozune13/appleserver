require('dotenv').config();
console.log('Environment variables loaded?', process.env.FIREBASE_PROJECT_ID ? 'YES ✅' : 'NO ❌');
const WebSocket = require('ws');
const admin = require('firebase-admin');
const jwt = require('jsonwebtoken');
const http = require('http');
const url = require('url');

// JWT Secret
const JWT_SECRET = process.env.JWT_SECRET || 'your-super-secret-jwt-key-change-in-production';
const TOKEN_EXPIRY = '30d';

// Firebase init
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

const server = http.createServer();
const wss = new WebSocket.Server({ server });

// In-memory caches
const activeConnections = new Map(); // ws -> { tag, connectionId, ..., role }
const ipConnectionCount = new Map();
const cubesCache = new Map();
const questionsCache = new Map();
const threadsCache = new Map();

const RATE_LIMIT_WINDOW = 60000;
const MAX_CONNECTIONS_PER_IP = 5;

// ---------- Permission helpers ----------
function canSeePrivate(conn) {
  return conn.role === 'admin' || conn.role === 'staff' || conn.role === 'peerMentor';
}
function canResolve(conn) {
  return conn.role === 'admin' || conn.role === 'staff' || conn.role === 'peerMentor';
}
function canBroadcast(conn) {
  return conn.role === 'admin' || conn.role === 'peerMentor';
}

// ---------- Load data on startup ----------
async function loadCubesFromFirebase() { /* ...same... */ }
async function loadQuestionsFromFirebase() { /* ...same... */ }
async function loadThreadsFromFirebase() { /* ...same... */ }

// ---------- WebSocket connection handling ----------
wss.on('connection', async (ws, req) => {
  const ip = req.socket.remoteAddress;
  // rate limiting
  if ((ipConnectionCount.get(ip) || 0) >= MAX_CONNECTIONS_PER_IP) {
    ws.close(1008, 'Too many connections');
    return;
  }
  ipConnectionCount.set(ip, (ipConnectionCount.get(ip) || 0) + 1);

  const connectionId = generateConnectionId();
  const token = jwt.sign({ connectionId, ip, iat: Date.now() }, JWT_SECRET, { expiresIn: TOKEN_EXPIRY });

  activeConnections.set(ws, {
    tag: null,
    ws,
    connectionId,
    ip,
    token,
    lastSeen: Date.now(),
    connectedAt: Date.now(),
    role: 'student'
  });

  ws.send(JSON.stringify({ type: 'connected', connectionId, token, message: 'Connected to Flock Server', timestamp: new Date().toISOString() }));

  connectionsCollection.doc(connectionId).set({
    ip, connectedAt: admin.firestore.FieldValue.serverTimestamp(),
    userAgent: req.headers['user-agent'] || 'unknown'
  }).catch(console.error);

  ws.on('message', async (data) => {
    try {
      const message = JSON.parse(data.toString());
      const connection = activeConnections.get(ws);
      if (!connection) return;
      connection.lastSeen = Date.now();

      switch (message.type) {
        case 'register_tag': await handleTagRegistration(ws, message.tag, message.role); break;
        case 'unregister_tag': await handleTagUnregistration(ws); break;
        case 'get_active_tags': await sendActiveTags(ws); break;
        case 'send_message': await handleMessage(ws, message, connection); break;
        case 'get_messages': await fetchMessages(ws, message.limit || 50); break;
        case 'cube_location': await handleCubeLocation(ws, message, connection); break;
        case 'get_cubes': await sendCubeList(ws); break;
        case 'send_question': case 'send_private_question': await handleQuestion(ws, message, connection); break;
        case 'broadcast_question': await handleBroadcastQuestion(ws, message, connection); break;
        case 'answer_broadcast': await handleAnswerBroadcast(ws, message, connection); break;
        case 'resolve_question': await handleResolveQuestion(ws, message, connection); break;
        case 'get_questions': await sendQuestionList(ws, connection); break;
        case 'create_thread': await handleCreateThread(ws, message, connection); break;
        case 'thread_message': await handleThreadMessage(ws, message, connection); break;
        case 'resolve_thread': await handleResolveThread(ws, message, connection); break;
        case 'update_role': await handleUpdateRole(ws, message, connection); break;
        case 'ping': ws.send(JSON.stringify({ type: 'pong', timestamp: Date.now() })); break;
        default: ws.send(JSON.stringify({ type: 'error', message: 'Unknown command' }));
      }
    } catch (e) {
      console.error('Error processing message:', e);
      ws.send(JSON.stringify({ type: 'error', message: 'Invalid message format' }));
    }
  });

  ws.on('close', async () => {
    ipConnectionCount.set(ip, Math.max(0, ipConnectionCount.get(ip) - 1));
    const conn = activeConnections.get(ws);
    if (conn) {
      if (conn.tag) await removeTag(conn.tag, conn.connectionId);
      await connectionsCollection.doc(conn.connectionId).update({
        disconnectedAt: admin.firestore.FieldValue.serverTimestamp()
      }).catch(console.error);
      activeConnections.delete(ws);
    }
  });
});

// ---------- Tag handlers (unchanged) ----------
async function handleTagRegistration(ws, tag, role) { /* existing code, validates roles incl. peerMentor */ }
async function handleTagUnregistration(ws) { /* existing */ }
async function removeTag(tag, connId) { /* existing */ }
async function sendActiveTags(ws) { /* existing */ }
async function broadcastActiveTags() { /* existing */ }

// ---------- Message handler (supports replies and threads) ----------
async function handleMessage(ws, message, connection) {
  if (!message.text || typeof message.text !== 'string') {
    ws.send(JSON.stringify({ type: 'message_response', success: false, message: 'Text required' }));
    return;
  }
  const trimmed = message.text.trim();
  if (!trimmed || trimmed.length > 1000) {
    ws.send(JSON.stringify({ type: 'message_response', success: false, message: 'Invalid length' }));
    return;
  }

  const msgData = {
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
    const docRef = await messagesCollection.add(msgData);
    ws.send(JSON.stringify({ type: 'message_response', success: true, messageId: docRef.id }));

    const broadcast = JSON.stringify({
      type: 'new_message',
      messageId: docRef.id,
      text: msgData.text,
      from: msgData.tag,
      connectionId: msgData.connectionId,
      timestamp: msgData.timestamp,
      replyToMessageId: msgData.replyToMessageId,
      threadId: msgData.threadId
    });

    activeConnections.forEach((conn, client) => {
      if (client.readyState === WebSocket.OPEN && conn.connectionId !== connection.connectionId) {
        client.send(broadcast);
      }
    });
  } catch (e) {
    console.error('Error storing message:', e);
    ws.send(JSON.stringify({ type: 'message_response', success: false, message: 'Server error' }));
  }
}

// ---------- Question / Broadcast handlers ----------

async function handleQuestion(ws, message, connection) {
  if (!connection.tag) return;
  const text = (message.text || '').trim();
  if (!text || text.length > 1000) return;

  const isPrivate = message.type === 'send_private_question';
  const qid = message.questionId || ('q_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6));

  const questionData = {
    questionId: qid,
    askedBy: connection.tag,
    targetUser: message.targetUser || null,
    text,
    isPrivate,
    isResolved: false,
    resolvedBy: null,
    resolvedAt: null,
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
    createdAtMs: Date.now(),
    isBroadcast: false
  };

  try {
    await questionsCollection.doc(qid).set(questionData);
    const cached = { ...questionData, createdAt: Date.now() / 1000 };
    questionsCache.set(qid, cached);
    ws.send(JSON.stringify({ type: 'question_received', success: true, questionId: qid }));
    broadcastQuestion(cached, connection.connectionId);
  } catch (e) { /* handle error */ }
}

function broadcastQuestion(q, senderConnId) {
  activeConnections.forEach((conn, client) => {
    if (client.readyState !== WebSocket.OPEN || conn.connectionId === senderConnId) return;
    if (q.isPrivate) {
      if (!canSeePrivate(conn) && !(q.targetUser && conn.tag === q.targetUser)) return;
    }
    client.send(JSON.stringify({
      type: q.isPrivate ? 'new_private_question' : 'new_question',
      questionId: q.questionId,
      askedBy: q.askedBy,
      targetUser: q.targetUser,
      text: q.text,
      isPrivate: q.isPrivate,
      isResolved: false,
      createdAt: q.createdAt
    }));
  });
}

async function handleBroadcastQuestion(ws, message, connection) {
  if (!canBroadcast(connection)) {
    ws.send(JSON.stringify({ type: 'error', message: 'Only peer mentors or admins can broadcast' }));
    return;
  }
  const text = (message.text || '').trim();
  if (!text || text.length > 1000) return;
  const qid = message.questionId || ('b_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6));

  const questionData = {
    questionId: qid,
    askedBy: connection.tag,
    targetUser: null,
    text,
    isPrivate: false,
    isResolved: false,
    resolvedBy: null,
    resolvedAt: null,
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
    createdAtMs: Date.now(),
    isBroadcast: true
  };

  try {
    await questionsCollection.doc(qid).set(questionData);
    const cached = { ...questionData, createdAt: Date.now() / 1000 };
    questionsCache.set(qid, cached);
    ws.send(JSON.stringify({ type: 'broadcast_sent', success: true, questionId: qid }));

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
  } catch (e) { /* handle error */ }
}

async function handleAnswerBroadcast(ws, message, connection) {
  if (!connection.tag) return;
  const { questionId, hasAnswered } = message;
  const ref = questionsCollection.doc(questionId).collection('answers').doc(connection.tag);
  try {
    await ref.set({ tag: connection.tag, hasAnswered, timestamp: admin.firestore.FieldValue.serverTimestamp() });
    const snapshot = await questionsCollection.doc(questionId).collection('answers').get();
    const answers = {};
    snapshot.forEach(doc => answers[doc.id] = doc.data().hasAnswered);
    const payload = JSON.stringify({ type: 'broadcast_answers', questionId, answers });
    activeConnections.forEach((conn, client) => {
      if (client.readyState === WebSocket.OPEN) client.send(payload);
    });
  } catch (e) { console.error(e); }
}

// ---------- Resolve question ----------
async function handleResolveQuestion(ws, message, connection) {
  if (!canResolve(connection)) return;
  const { questionId, resolvedBy } = message;
  try {
    await questionsCollection.doc(questionId).update({
      isResolved: true,
      resolvedBy: resolvedBy || connection.tag,
      resolvedAt: admin.firestore.FieldValue.serverTimestamp()
    });
    if (questionsCache.has(questionId)) {
      questionsCache.get(questionId).isResolved = true;
    }
    const payload = JSON.stringify({ type: 'question_resolved', questionId, resolvedBy: resolvedBy || connection.tag, timestamp: Date.now() });
    ws.send(payload);
    activeConnections.forEach((conn, client) => {
      if (client.readyState === WebSocket.OPEN && conn.connectionId !== connection.connectionId) {
        client.send(payload);
      }
    });
  } catch (e) { /* handle error */ }
}

// ---------- Send question list (includes isBroadcast) ----------
async function sendQuestionList(ws, connection) {
  const questions = [];
  questionsCache.forEach(q => {
    if (q.isResolved) return;
    if (q.isPrivate && !canSeePrivate(connection) && !(q.askedBy === connection.tag || q.targetUser === connection.tag)) return;
    questions.push({
      questionId: q.questionId,
      askedBy: q.askedBy,
      targetUser: q.targetUser,
      text: q.text,
      isPrivate: q.isPrivate,
      isResolved: q.isResolved,
      resolvedBy: q.resolvedBy,
      createdAt: q.createdAt,
      isBroadcast: q.isBroadcast || false
    });
  });
  ws.send(JSON.stringify({ type: 'question_list', questions, count: questions.length, timestamp: Date.now() }));
}

// ---------- Thread handlers ----------
async function handleCreateThread(ws, message, connection) {
  const { questionId } = message;
  const threadId = 'thread_' + questionId;
  if (threadsCache.has(threadId)) {
    ws.send(JSON.stringify({ type: 'thread_created', ...threadsCache.get(threadId) }));
    return;
  }
  const question = questionsCache.get(questionId);
  if (!question) return;
  const threadData = {
    id: threadId,
    questionId,
    title: `Question #${questionId.slice(-4)}`,
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
    createdAtMs: Date.now(),
    isResolved: false,
    resolvedBy: null,
    resolvedAt: null,
    messages: []
  };
  try {
    await threadsCollection.doc(threadId).set(threadData);
    threadsCache.set(threadId, threadData);
    await questionsCollection.doc(questionId).update({ threadId: threadId });
    question.threadId = threadId;
    const payload = JSON.stringify({ type: 'thread_created', ...threadData });
    ws.send(payload);
    activeConnections.forEach((conn, client) => {
      if (client.readyState === WebSocket.OPEN && conn.connectionId !== connection.connectionId) {
        client.send(payload);
      }
    });
  } catch (e) { /* handle error */ }
}

async function handleThreadMessage(ws, message, connection) {
  const { threadId, text } = message;
  const thread = threadsCache.get(threadId);
  if (!thread || thread.isResolved) return;
  const msg = {
    id: 'tm_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6),
    author: connection.tag,
    text: text.trim(),
    timestamp: admin.firestore.FieldValue.serverTimestamp(),
    timestampMs: Date.now()
  };
  try {
    await threadsCollection.doc(threadId).update({ messages: admin.firestore.FieldValue.arrayUnion(msg) });
    thread.messages.push(msg);
    ws.send(JSON.stringify({ type: 'thread_message_sent', threadId, message: msg }));
    const payload = JSON.stringify({ type: 'thread_message_added', threadId, message: msg });
    activeConnections.forEach((conn, client) => {
      if (client.readyState === WebSocket.OPEN && conn.connectionId !== connection.connectionId) {
        client.send(payload);
      }
    });
  } catch (e) { /* handle error */ }
}

async function handleResolveThread(ws, message, connection) {
  const { threadId, resolvedBy } = message;
  const thread = threadsCache.get(threadId);
  if (!thread) return;
  const isCreator = thread.messages[0]?.author === connection.tag;
  if (!isCreator && !canResolve(connection)) return;
  try {
    await threadsCollection.doc(threadId).update({
      isResolved: true,
      resolvedBy: resolvedBy || connection.tag,
      resolvedAt: admin.firestore.FieldValue.serverTimestamp()
    });
    thread.isResolved = true;
    const payload = JSON.stringify({ type: 'thread_resolved', threadId, resolvedBy: resolvedBy || connection.tag });
    ws.send(payload);
    activeConnections.forEach((conn, client) => {
      if (client.readyState === WebSocket.OPEN && conn.connectionId !== connection.connectionId) {
        client.send(payload);
      }
    });
  } catch (e) { /* handle error */ }
}

// ---------- Role update ----------
async function handleUpdateRole(ws, message, connection) {
  // existing code – allows student<->peerMentor toggle, admin/staff can set others
}

// ---------- Cube helpers (unchanged) ----------
async function handleCubeLocation(ws, message, connection) { /* ... */ }
async function sendCubeList(ws) { /* ... */ }

// ---------- Utility ----------
function generateConnectionId() { return 'conn_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9); }

// ---------- Heartbeat and cleanup ----------
setInterval(() => {
  const now = Date.now();
  activeConnections.forEach((conn, ws) => {
    if (now - conn.lastSeen > 30000 && ws.readyState === WebSocket.OPEN) {
      ws.close(1000, 'Connection timeout');
    }
  });
}, 10000);

process.on('SIGINT', async () => {
  // graceful shutdown
});

const PORT = process.env.PORT || 8080;
Promise.all([loadCubesFromFirebase(), loadQuestionsFromFirebase(), loadThreadsFromFirebase()]).then(() => {
  server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
}).catch(err => {
  console.error('Startup error', err);
  server.listen(PORT, () => console.log(`Server running on port ${PORT} (initial load failed)`));
});