require('dotenv').config();
const path = require('path');
const fs = require('fs');
const http = require('http');
const express = require('express');
const { v4: uuidv4 } = require('uuid');
const { pool } = require('./db');
const { setupWebSocketServer } = require('./wsServer');
const { runExport } = require('./exportWorker');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, '..', 'public')));

const PORT = parseInt(process.env.PORT || '8080', 10);

app.get('/health', (req, res) => {
  res.status(200).json({ status: 'ok' });
});

// POST /api/exports - create + queue a new export job, return 202 immediately
app.post('/api/exports', async (req, res) => {
  try {
    const exportId = uuidv4();
    await pool.query(
      `INSERT INTO exports (export_id, status) VALUES ($1, 'queued')`,
      [exportId]
    );

    res.status(202).json({ exportId });

    // Fire the worker asynchronously; it does not block the API response.
    setImmediate(() => {
      runExport(exportId).catch((err) => {
        console.error(`[export ${exportId}] unhandled worker error:`, err);
      });
    });
  } catch (err) {
    console.error('POST /api/exports failed:', err);
    res.status(500).json({ error: 'Failed to create export job' });
  }
});

// GET /api/exports - last 20 jobs
app.get('/api/exports', async (req, res) => {
  try {
    const { rows } = await pool.query(
      `SELECT export_id, status, created_at, completed_at
       FROM exports ORDER BY created_at DESC LIMIT 20`
    );
    res.status(200).json({
      exports: rows.map((r) => ({
        exportId: r.export_id,
        status: r.status,
        createdAt: r.created_at ? r.created_at.toISOString() : null,
        completedAt: r.completed_at ? r.completed_at.toISOString() : null,
      })),
    });
  } catch (err) {
    console.error('GET /api/exports failed:', err);
    res.status(500).json({ error: 'Failed to list exports' });
  }
});

// GET /api/exports/:exportId/download
app.get('/api/exports/:exportId/download', async (req, res) => {
  try {
    const { exportId } = req.params;
    const { rows } = await pool.query(
      `SELECT * FROM exports WHERE export_id = $1`,
      [exportId]
    );

    if (rows.length === 0) {
      return res.status(404).json({ error: 'Export not found' });
    }

    const job = rows[0];
    if (job.status !== 'completed' || !job.file_path || !fs.existsSync(job.file_path)) {
      return res.status(409).json({ error: 'Export is not ready for download' });
    }

    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename="export-${exportId}.csv"`);
    fs.createReadStream(job.file_path).pipe(res);
  } catch (err) {
    console.error('GET download failed:', err);
    res.status(500).json({ error: 'Failed to download export' });
  }
});

const server = http.createServer(app);
setupWebSocketServer(server);

server.listen(PORT, () => {
  console.log(`CSV export service listening on port ${PORT}`);
});
