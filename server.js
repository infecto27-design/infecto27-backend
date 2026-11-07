const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { Pool } = require('pg');
const cors = require('cors');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

app.get('/', (req, res) => {
  res.send('NexusConnect Backend radi uspešno!');
});

app.get('/api/messages/:channel', async (req, res) => {
  try {
    const { channel } = req.params;
    const result = await pool.query(
      `SELECT m.id, u.name as user, m.content, TO_CHAR(m.created_at, 'HH24:MI') as timestamp 
       FROM messages m JOIN users u ON m.user_id = u.id 
       WHERE m.channel_name = $1 ORDER BY m.created_at ASC`,
      [channel]
    );
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

io.on('connection', (socket) => {
  socket.on('join_channel', async (channel) => {
    socket.join(channel);
    try {
      const result = await pool.query(
        `SELECT m.id, u.name as user, m.content, TO_CHAR(m.created_at, 'HH24:MI') as timestamp 
         FROM messages m JOIN users u ON m.user_id = u.id 
         WHERE m.channel_name = $1 ORDER BY m.created_at ASC`,
        [channel]
      );
      socket.emit('load_messages', result.rows);
    } catch (err) {
      console.error(err);
    }
  });

  socket.on('send_message', async (data) => {
    const { channel, userId, content } = data;
    try {
      const insertRes = await pool.query(
        `INSERT INTO messages (channel_name, user_id, content) VALUES ($1, $2, $3) RETURNING id, created_at`,
        [channel, userId, content]
      );
      
      const userRes = await pool.query(`SELECT name FROM users WHERE id = $1`, [userId]);

      const newMessage = {
        id: insertRes.rows[0].id,
        user: userRes.rows[0].name,
        content,
        timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
      };

      io.to(channel).emit('receive_message', newMessage);
    } catch (err) {
      console.error("Greška pri čuvanju poruke:", err);
    }
  });
});

const PORT = process.env.PORT || 5000;
server.listen(PORT, () => {
  console.log(`Server radi na portu ${PORT}`);
});
