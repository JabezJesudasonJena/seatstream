const express = require('express');
const { Client } = require('cassandra-driver');
const { Client: ESClient } = require('@elastic/elasticsearch');
const { v4: uuidv4 } = require('uuid');

// Kafka
const { Kafka } = require('kafkajs');
const { Partitioners } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'movie-api',
  brokers: ['kafka:9092'] // Connects to the broker on the Docker network
});


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
app.post('/api/v1/movie/book/id/:movieId', async (req, res) => {
  const { movieId } = req.params;
  const { seatNumber, userId } = req.body;

  try {
    const query = `
        UPDATE movie_reservation.seat_reservations 
        SET status = 'booked', user_id = ? 
        WHERE movie_id = ? AND seat_number = ? 
        IF status = 'available'
    `;
    
    const result = await cassandra.execute(query, [userId, movieId, seatNumber], { prepare: true });

    // result.rows[0].get('[applied]') returns true if the IF condition was met
    const applied = result.rows[0].get('[applied]');

    if (applied) {
      res.json({ success: true, message: 'Seat locked. Proceed to payment.' });
    } else {
      res.status(409).json({ success: false, error: 'Seat is already booked or locked.' });
    }
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

async function startServer() {
  let retries = 10;
  while (retries) {
    try {
      await initDB();
      await producer.connect(); // <-- Connect Kafka
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