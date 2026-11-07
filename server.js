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

// Podešavanje email transportera (možeš podesiti svoje SMTP podatke u .env)
const transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: process.env.EMAIL_USER || 'tvoj-email@gmail.com',
    pass: process.env.EMAIL_PASS || 'tvoja-lozinka-aplikacije'
  }
});

async function sendEmailNotification(to, subject, text) {
  if (!process.env.EMAIL_USER) return; // Preskače ako nisu podešeni kredencijali
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
        amount NUMERIC(12,2) DEFAULT 0,
        file_data TEXT NOT NULL,
        file_name VARCHAR(255) NOT NULL,
        uploaded_by INT REFERENCES users(id),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `);
    
    const userCheck = await pool.query('SELECT COUNT(*) FROM users');
    if (parseInt(userCheck.rows[0].count) === 0) {
      await pool.query(`INSERT INTO users (name, email, role) VALUES ('Jovan (Zaposleni)', 'jovan@firma.com', 'employee')`);
    }
  } catch (err) {
    console.error("Greška pri inicijalizaciji baze:", err);
  }
}
initDB();

app.get('/', (req, res) => {
  res.send('NexusConnect ERP Backend radi sa emailom i notifikacijama!');
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
      `SELECT d.id, d.title, d.category, d.amount, d.file_data, d.file_name, u.name as uploader, TO_CHAR(d.created_at, 'DD.MM.YYYY HH24:MI') as timestamp 
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
    const { title, category, amount, fileData, fileName, userId } = req.body;
    const result = await pool.query(
      `INSERT INTO documents (title, category, amount, file_data, file_name, uploaded_by) VALUES ($1, $2, $3, $4, $5, $6) RETURNING *`,
      [title, category, amount || 0, fileData, fileName, userId]
    );

    // Notifikacija u realnom vremenu svim konektovanim korisnicima
    io.emit('new_notification', {
      type: 'document',
      text: `Novi dokument sačuvan: ${title} (${amount} RSD)`
    });

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

      // Notifikacija za nove poruke izvan kanala
      socket.broadcast.emit('new_notification', {
        type: 'chat',
        text: `Nova poruka od ${newMessage.user} u #${channel}`
      });
    } catch (err) { console.error("Greška pri čuvanju poruke:", err); }
  });

  socket.on('assign_task', (data) => {
    io.emit('new_notification', {
      type: 'task',
      text: `Novi zadatak dodeljen: ${data.title} (${data.assignee})`
    });
    // Šalje email notifikaciju ako imamo email primaoca
    if (data.assigneeEmail) {
      sendEmailNotification(data.assigneeEmail, 'Novi zadatak u NexusConnect-u', `Dodeljen vam je zadatak: ${data.title}\nDetalji: ${data.desc}`);
    }
  });
});

const PORT = process.env.PORT || 5000;
server.listen(PORT, () => {
  console.log(`ERP Server radi na portu ${PORT}`);
});
