const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { Pool } = require('pg');
const cors = require('cors');
const nodemailer = require('nodemailer');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json({ limit: '15mb' }));

const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

const transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: process.env.EMAIL_USER || 'tvoj-email@gmail.com',
    pass: process.env.EMAIL_PASS || 'tvoja-lozinka-aplikacije'
  }
});

async function sendEmailNotification(to, subject, text) {
  if (!process.env.EMAIL_USER) return;
  try {
    await transporter.sendMail({
      from: `"NexusConnect ERP" <${process.env.EMAIL_USER}>`,
      to, subject, text
    });
  } catch (err) {
    console.error("Greška pri slanju emaila:", err.message);
  }
}

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
        type VARCHAR(20) DEFAULT 'Rashod', -- 'Rashod' ili 'Prihod'
        amount NUMERIC(12,2) DEFAULT 0,
        status VARCHAR(50) DEFAULT 'Na čekanju',
        file_data TEXT NOT NULL,
        file_name VARCHAR(255) NOT NULL,
        uploaded_by INT REFERENCES users(id),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
      CREATE TABLE IF NOT EXISTS contracts (
        id SERIAL PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        partner VARCHAR(255) NOT NULL,
        expiry_date DATE NOT NULL,
        status VARCHAR(50) DEFAULT 'Aktivan',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `);
    
    const userCheck = await pool.query('SELECT COUNT(*) FROM users');
    if (parseInt(userCheck.rows[0].count) === 0) {
      await pool.query(`INSERT INTO users (name, email, role) VALUES ('Jovan (Admin)', 'jovan@firma.com', 'admin')`);
    }
  } catch (err) {
    console.error("Greška pri inicijalizaciji baze:", err);
  }
}
initDB();

app.get('/', (req, res) => {
  res.send('NexusConnect Enterprise ERP Backend radi uspešno!');
});

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

app.get('/api/users', async (req, res) => {
  try {
    const result = await pool.query(`SELECT * FROM users ORDER BY id ASC`);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/documents', async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT d.id, d.title, d.category, d.type, d.amount, d.status, d.file_data, d.file_name, u.name as uploader, TO_CHAR(d.created_at, 'DD.MM.YYYY HH24:MI') as timestamp 
       FROM documents d JOIN users u ON d.uploaded_by = u.id 
       ORDER BY d.created_at DESC`
    );
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/documents', async (req, res) => {
  try {
    const { title, category, type, amount, fileData, fileName, userId } = req.body;
    const result = await pool.query(
      `INSERT INTO documents (title, category, type, amount, status, file_data, file_name, uploaded_by) VALUES ($1, $2, $3, $4, 'Na čekanju', $5, $6, $7) RETURNING *`,
      [title, category, type || 'Rashod', amount || 0, fileData, fileName, userId]
    );

    io.emit('new_notification', {
      type: 'document',
      text: `Novi ${type || 'Rashod'}: ${title} (${amount} RSD) - Čeka odobrenje`
    });

    res.json(result.rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.patch('/api/documents/:id/status', async (req, res) => {
  try {
    const { id } = req.params;
    const { status } = req.body;
    const result = await pool.query(
      `UPDATE documents SET status = $1 WHERE id = $2 RETURNING *`,
      [status, id]
    );
    io.emit('new_notification', {
      type: 'status',
      text: `Status dokumenta #${id} promenjen u: ${status}`
    });
    res.json(result.rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Ugovori rute
app.get('/api/contracts', async (req, res) => {
  try {
    const result = await pool.query(`SELECT * FROM contracts ORDER BY expiry_date ASC`);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/contracts', async (req, res) => {
  try {
    const { title, partner, expiryDate } = req.body;
    const result = await pool.query(
      `INSERT INTO contracts (title, partner, expiry_date) VALUES ($1, $2, $3) RETURNING *`,
      [title, partner, expiryDate]
    );
    res.json(result.rows[0]);
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
    } catch (err) { console.error(err); }
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
      socket.broadcast.emit('new_notification', {
        type: 'chat',
        text: `Nova poruka od ${newMessage.user} u #${channel}`
      });
    } catch (err) { console.error(err); }
  });

  socket.on('assign_task', (data) => {
    io.emit('new_notification', {
      type: 'task',
      text: `Novi zadatak: ${data.title} (${data.assignee})`
    });
    if (data.assigneeEmail) {
      sendEmailNotification(data.assigneeEmail, 'Novi zadatak', `Dodeljen vam je zadatak: ${data.title}`);
    }
  });
});

const PORT = process.env.PORT || 5000;
server.listen(PORT, () => {
  console.log(`Server radi na portu ${PORT}`);
});
