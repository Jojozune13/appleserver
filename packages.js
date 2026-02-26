{
  "name": "ar-tag-server",
  "version": "1.0.0",
  "description": "WebSocket server for AR gamertags with Firebase",
  "main": "server.js",
  "scripts": {
    "start": "node server.js",
    "dev": "nodemon server.js"
  },
  "dependencies": {
    "ws": "^8.18.1",
    "firebase-admin": "^11.11.0",
    "dotenv": "^16.4.7"
  },
  "devDependencies": {
    "nodemon": "^3.1.7"
  }
}