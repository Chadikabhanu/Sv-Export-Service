const Redis = require('ioredis');

const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';

// Separate clients: ioredis puts a client into subscriber-only mode once you
// call subscribe(), so a distinct client is needed for regular commands.
const publisher = new Redis(REDIS_URL);
const subscriber = new Redis(REDIS_URL);

publisher.on('error', (err) => console.error('[redis:publisher]', err.message));
subscriber.on('error', (err) => console.error('[redis:subscriber]', err.message));

function channelFor(exportId) {
  return `export-progress:${exportId}`;
}

async function publishProgress(exportId, payload) {
  await publisher.publish(channelFor(exportId), JSON.stringify(payload));
}

// exportId -> Set<WebSocket>
const subscribers = new Map();

subscriber.on('message', (channel, message) => {
  const exportId = channel.replace('export-progress:', '');
  const sockets = subscribers.get(exportId);
  if (!sockets) return;
  for (const ws of sockets) {
    if (ws.readyState === ws.OPEN) {
      ws.send(message);
    }
  }
});

async function addSocket(exportId, ws) {
  let sockets = subscribers.get(exportId);
  if (!sockets) {
    sockets = new Set();
    subscribers.set(exportId, sockets);
    await subscriber.subscribe(channelFor(exportId));
  }
  sockets.add(ws);
}

async function removeSocket(exportId, ws) {
  const sockets = subscribers.get(exportId);
  if (!sockets) return;
  sockets.delete(ws);
  if (sockets.size === 0) {
    subscribers.delete(exportId);
    await subscriber.unsubscribe(channelFor(exportId));
  }
}

// Cancellation flags are kept in Redis (not just in-memory) so that the
// worker process could run separately from the WS process in a scaled setup.
function cancelKey(exportId) {
  return `export-cancel:${exportId}`;
}

async function requestCancel(exportId) {
  await publisher.set(cancelKey(exportId), '1', 'EX', 3600);
}

async function isCancelled(exportId) {
  const val = await publisher.get(cancelKey(exportId));
  return val === '1';
}

module.exports = {
  publisher,
  subscriber,
  publishProgress,
  addSocket,
  removeSocket,
  requestCancel,
  isCancelled,
};
