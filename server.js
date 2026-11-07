const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { Pool } = require('pg');
const cors = require('cors');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json({ limit: '10mb' })); // Povećano ograničenje za upload slika i fajlova

const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

// Provera i automatsko kreiranje tabela ako ne postoje
async function initDB() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        role VARCHAR(50) DEFAULT 'employee',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
      CREATE TABLE IF NOT EXISTS messages (
        id SERIAL PRIMARY KEY,
        channel_name VARCHAR(50) NOT NULL,
        user_id INT REFERENCES users(id),
        content TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
      CREATE TABLE IF NOT EXISTS documents (
        id SERIAL PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        category VARCHAR(100) NOT NULL,
        file_data TEXT NOT NULL,
        file_name VARCHAR(255) NOT NULL,
        uploaded_by INT REFERENCES users(id),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `);
    
    // Ubaci podrazumevanog korisnika ako nema nijednog
    const userCheck = await pool.query('SELECT COUNT(*) FROM users');
    if (parseInt(userCheck.rows[0].count) === 0) {
      await pool.query(`INSERT INTO users (name, email, role) VALUES ('Jovan (Zaposleni)', 'jovan@firma.com', 'employee')`);
    }
    console.log("Baza uspešno inicijalizovana!");
  } catch (err) {
    console.error("Greška pri inicijalizaciji baze:", err);
  }
}
initDB();

app.get('/', (req, res) => {
  res.send('NexusConnect Backend radi uspešno sa dokumentima i korisnicima!');
});

// Ruta za dodavanje novog korisnika
app.post('/api/users', async (req, res) => {
  try {
    const { name, email, role } = req.body;
    const result = await pool.query(
      `INSERT INTO users (name, email, role) VALUES ($1, $2, $3) RETURNING *`,
      [name, email, role || 'employee']
    );
    res.json(result.rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Ruta za preuzimanje liste korisnika
app.get('/api/users', async (req, res) => {
  try {
    const result = await pool.query(`SELECT * FROM users ORDER BY id ASC`);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Ruta za preuzimanje dokumenata/faktura/računa
app.get('/api/documents', async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT d.id, d.title, d.category, d.file_data, d.file_name, u.name as uploader, TO_CHAR(d.created_at, 'DD.MM.YYYY HH24:MI') as timestamp 
       FROM documents d JOIN users u ON d.uploaded_by = u.id 
       ORDER BY d.created_at DESC`
    );
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Ruta za upload novog dokumenta/računa/otpremnice
app.post('/api/documents', async (req, res) => {
  try {
    const { title, category, fileData, fileName, userId } = req.body;
    const result = await pool.query(
      `INSERT INTO documents (title, category, file_data, file_name, uploaded_by) VALUES ($1, $2, $3, $4, $5) RETURNING *`,
      [title, category, fileData, fileName, userId]
    );
    res.json(result.rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Socket.io za real-time chat
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
