const { WebSocketServer } = require('ws');
const { pool } = require('./db');
const { addSocket, removeSocket, requestCancel } = require('./redis');

const HEARTBEAT_INTERVAL_MS = 30000;

function setupWebSocketServer(server) {
  const wss = new WebSocketServer({ noServer: true });

  server.on('upgrade', (request, socket, head) => {
    const match = request.url.match(/^\/ws\/exports\/([^/?]+)/);
    if (!match) {
      socket.destroy();
      return;
    }
    const exportId = match[1];

    wss.handleUpgrade(request, socket, head, (ws) => {
      ws.exportId = exportId;
      wss.emit('connection', ws, request);
    });
  });

  wss.on('connection', async (ws) => {
    const { exportId } = ws;
    ws.isAlive = true;

    // ws lib auto-responds to protocol-level ping frames with pong frames,
    // but we track pong receipt ourselves so the server can detect and
    // terminate stale connections.
    ws.on('pong', () => {
      ws.isAlive = true;
    });

    try {
      // On (re)connect, immediately replay current job status so a client
      // that reopened a tab isn't left waiting for the next live update.
      const { rows } = await pool.query(
        'SELECT * FROM exports WHERE export_id = $1',
        [exportId]
      );

      if (rows.length === 0) {
        ws.send(JSON.stringify({ exportId, status: 'not_found', timestamp: new Date().toISOString() }));
      } else {
        const job = rows[0];
        if (job.status === 'completed') {
          ws.send(JSON.stringify({
            exportId,
            status: 'completed',
            downloadUrl: `/api/exports/${exportId}/download`,
            fileSize: Number(job.file_size) || 0,
            durationSeconds: job.completed_at && job.started_at
              ? Math.round((new Date(job.completed_at) - new Date(job.started_at)) / 1000)
              : null,
          }));
        } else if (job.status === 'failed') {
          ws.send(JSON.stringify({
            exportId, status: 'failed', error: job.error, timestamp: new Date().toISOString(),
          }));
        } else if (job.status === 'cancelled') {
          ws.send(JSON.stringify({ exportId, status: 'cancelled', timestamp: new Date().toISOString() }));
        } else {
          const total = job.total || 0;
          const processed = job.processed || 0;
          ws.send(JSON.stringify({
            exportId,
            status: job.status, // queued | processing
            progress: {
              total,
              processed,
              percentage: total > 0 ? Math.round((processed / total) * 10000) / 100 : 0,
              etaSeconds: null,
            },
            timestamp: new Date().toISOString(),
          }));
        }
      }

      await addSocket(exportId, ws);
    } catch (err) {
      console.error(`[ws ${exportId}] setup error:`, err.message);
    }

    ws.on('message', async (raw) => {
      let msg;
      try {
        msg = JSON.parse(raw.toString());
      } catch (_) {
        return; // ignore non-JSON application messages
      }

      if (msg.action === 'cancel') {
        await requestCancel(exportId);
        // The worker checks the cancel flag between chunks and will publish
        // the authoritative "cancelled" message via Redis, which reaches
        // every subscriber for this exportId (including this socket).
      }
    });

    ws.on('close', async () => {
      await removeSocket(exportId, ws);
    });
  });

  // Server-initiated heartbeat: ping every connected client; terminate any
  // client that didn't respond with a pong since the last check.
  const interval = setInterval(() => {
    wss.clients.forEach((ws) => {
      if (ws.isAlive === false) {
        return ws.terminate();
      }
      ws.isAlive = false;
      ws.ping();
    });
  }, HEARTBEAT_INTERVAL_MS);

  wss.on('close', () => clearInterval(interval));

  return wss;
}

module.exports = { setupWebSocketServer };
