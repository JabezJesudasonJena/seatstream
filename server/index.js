const express = require('express');
const { Client } = require('cassandra-driver');
const { Client: ESClient } = require('@elastic/elasticsearch');
const { v4: uuidv4 } = require('uuid');
const { createClient } = require('redis');

// Kafka
const { Kafka } = require('kafkajs');
const { Partitioners } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'movie-api',
  brokers: ['kafka:9092'] // Connects to the broker on the Docker network
});

// redis 
const redisClient = createClient({ url: 'redis://redis:6379' });
const redisSubscriber = redisClient.duplicate();

// Redis initialize
async function initRedis() {
  await redisClient.connect();
  await redisSubscriber.connect();
  console.log('Redis Connected');

  // Listen for any key that expires
  await redisSubscriber.subscribe('__keyevent@0__:expired', async (key) => {
    // Keys will be formatted as "lock:movieId:seatNumber"
    if (key.startsWith('lock:')) {
      const [, movieId, seatNumber] = key.split(':');
      
      console.log(`Lock expired for Seat ${seatNumber}. Reverting to available.`);
      
      // Revert the seat in Cassandra
      await cassandra.execute(
        `UPDATE movie_reservation.seat_reservations 
         SET status = 'available' 
         WHERE movie_id = ? AND seat_number = ? IF status = 'locked'`,
        [movieId, seatNumber],
        { prepare: true }
      );
    }
  });
}

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner
});

const app = express();
app.use(express.json());

// 1. Initialize Clients
const cassandra = new Client({
  contactPoints: ['cassandra'], // Use 'cassandra' if running Node in Docker
  localDataCenter: 'datacenter1'
});

const es = new ESClient({
  node: 'http://elasticsearch:9200' // Use 'http://elasticsearch:9200' if in Docker
});

// 2. Initialize Cassandra Schema (Run once on startup)
async function initDB() {
  await cassandra.execute(`
    CREATE KEYSPACE IF NOT EXISTS movie_reservation 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
  `);
  
  await cassandra.execute(`
    CREATE TABLE IF NOT EXISTS movie_reservation.movies (
      movie_id UUID PRIMARY KEY,
      title TEXT,
      description TEXT
    );
  `);

  await cassandra.execute(`
    CREATE TABLE IF NOT EXISTS movie_reservation.seat_reservations (
      movie_id UUID,
      seat_number TEXT,
      status TEXT,
      user_id UUID,
      PRIMARY KEY (movie_id, seat_number)
    );
  `);
  console.log("Cassandra Schema Initialized");
}

// 3. Endpoints

// POST /api/v1/new: Add a movie and initialize seats
app.post('/api/v1/new', async (req, res) => {
  const { title, description, totalSeats } = req.body;
  const movieId = uuidv4();

  try {
    // 1. Insert into Cassandra
    await cassandra.execute(
      'INSERT INTO movie_reservation.movies (movie_id, title, description) VALUES (?, ?, ?)',
      [movieId, title, description],
      { prepare: true }
    );

    const queries = [];
    for (let i = 1; i <= totalSeats; i++) {
      queries.push({
        query: 'INSERT INTO movie_reservation.seat_reservations (movie_id, seat_number, status) VALUES (?, ?, ?)',
        params: [movieId, `S${i}`, 'available']
      });
    }
    await cassandra.batch(queries, { prepare: true });

    // 2. Publish Event to Kafka
    // The Elasticsearch Sink Connector will instantly pick this up
    await producer.send({
      topic: 'movies_topic',
      messages: [
        { 
          value: JSON.stringify({ 
            movie_id: movieId, 
            title: title, 
            description: description 
          }) 
        }
      ],
    });

    res.status(201).json({ movieId, message: 'Movie created, seats initialized, and pushed to Kafka' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// GET /api/v1/movie?q: Search movies via Elasticsearch
// GET /api/v1/movie?q: Search movies via Elasticsearch
app.get('/api/v1/movie', async (req, res) => {
  const { q } = req.query;
  try {
    const result = await es.search({
      index: 'movies_topic', // <-- CHANGE THIS FROM 'movies'
      query: {
        multi_match: {
          query: q,
          fields: ['title', 'description']
        }
      }
    });
    res.json(result.hits.hits.map(hit => hit._source));
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// GET /api/v1/movie/id/:id: Get single movie details from Elasticsearch
// GET /api/v1/movie/id/:id: Get single movie details from Elasticsearch
app.get('/api/v1/movie/id/:id', async (req, res) => {
  try {
    const result = await es.get({
      index: 'movies_topic', // <-- CHANGE THIS FROM 'movies'
      id: req.params.id
    });
    res.json(result._source);
  } catch (error) {
    res.status(404).json({ error: 'Movie not found' });
  }
});

// POST /api/v1/movie/book/id/:movieId: Book a seat using Lightweight Transactions (LWT)
// POST /api/v1/movie/book/id/:movieId
app.post('/api/v1/movie/book/id/:movieId', async (req, res) => {
  const { movieId } = req.params;
  const { seatNumber, userId } = req.body;
  const redisKey = `lock:${movieId}:${seatNumber}`;

  try {
    // 1. FAST PATH: Acquire Redis Mutex Lock
    const acquiredLock = await redisClient.set(redisKey, userId, { 
      EX: 600, 
      NX: true 
    });

    if (!acquiredLock) {
      return res.status(409).json({ success: false, error: 'Seat is currently on hold.' });
    }

    // 2. PERSISTENT STATE: Sync the lock to Cassandra for the frontend
    const query = `
        UPDATE movie_reservation.seat_reservations 
        SET status = 'locked', user_id = ? 
        WHERE movie_id = ? AND seat_number = ? 
        IF status = 'available'
    `;
    
    const result = await cassandra.execute(query, [userId, movieId, seatNumber], { prepare: true });

    // 3. ROLLBACK IF DB FAILS: If Cassandra says it wasn't available, drop the Redis lock
    if (!result.rows[0].get('[applied]')) {
      await redisClient.del(redisKey);
      return res.status(409).json({ success: false, error: 'Seat is already permanently booked in DB.' });
    }

    return res.status(200).json({ 
      success: true, 
      message: "Seat locked. You have 10 minutes to complete payment." 
    });
  } catch (error) {
    // Failsafe: if the Node server crashes here, drop the Redis lock so it doesn't hang
    await redisClient.del(redisKey);
    return res.status(500).json({ error: error.message });
  }
});

app.post('/api/v1/movie/pay/id/:id', async (req, res) => {
  const movieId = req.params.id;
  const { seatNumber, userId, paymentToken } = req.body;
  const redisKey = `lock:${movieId}:${seatNumber}`;

  try {
    // 1. Verify user still holds the Redis lock
    const lockedUserId = await redisClient.get(redisKey);
    if (lockedUserId !== userId) {
      return res.status(400).json({ success: false, error: "Lock expired or owned by someone else." });
    }

    // 2. Process Payment Gateway (Mocked)
    if (!paymentToken) throw new Error("Payment failed");

    // 3. Write permanently to Cassandra using LWT to ensure it's actually free
    // const query = `
    //     UPDATE movie_reservation.seat_reservations 
    //     SET status = 'booked', user_id = ? 
    //     WHERE movie_id = ? AND seat_number = ? 
    //     IF status = 'available'
    // `;
    // Inside your /pay endpoint:
    const query = `
        UPDATE movie_reservation.seat_reservations 
        SET status = 'booked', user_id = ? 
        WHERE movie_id = ? AND seat_number = ? 
        IF status = 'locked'   
    `;
    const result = await cassandra.execute(query, [userId, movieId, seatNumber], { prepare: true });

    if (!result.rows[0].get('[applied]')) {
      // Edge case: Cassandra says it was already booked
      return res.status(409).json({ success: false, error: "Seat was already booked." });
    }

    // 4. Cleanup Redis lock
    await redisClient.del(redisKey);

    return res.status(200).json({ success: true, message: "Seat permanently booked!" });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

async function startServer() {
  let retries = 10;
  while (retries) {
    try {
      await initDB();
      await producer.connect(); // <-- Connect Kafka
      await initRedis();
      console.log("Kafka Producer Connected");
      break;
    } catch (err) {
      console.log(`Databases not ready. Retrying...`);
      retries -= 1;
      await new Promise(res => setTimeout(res, 5000));
    }
  }
  
  app.listen(3000, () => {
    console.log('Server running on port 3000');
  });
}
// Add this at the very bottom of index.js
startServer();