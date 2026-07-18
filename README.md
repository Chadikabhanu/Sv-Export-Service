# CSV Export Service

Streams a 120,000-row `users` table to CSV in the background and pushes
live progress to the browser over WebSockets, decoupled through Redis
Pub/Sub.

## Architecture

```
Browser  <──WebSocket──>  App (Express + ws)  <──Pub/Sub──>  Redis
                               │      ▲
                          HTTP API    │ subscribes per exportId
                               │      │
                               ▼      │
                          Export worker (streams rows, writes CSV,
                          publishes progress every chunk)
                               │
                               ▼
                          Postgres (users table + exports job table)
```

- **app**: single Node process. Express handles the REST API; a `ws`
  WebSocketServer shares the same HTTP server and handles
  `/ws/exports/:exportId` connections.
- **Export worker** (`src/exportWorker.js`): runs asynchronously (fired
  with `setImmediate`, never blocks the request). It pages through
  `users` in chunks of 10,000 rows using keyset pagination (`WHERE id >
  lastId`), streams each chunk to a CSV file on disk, and after every
  chunk publishes a progress message to the Redis channel
  `export-progress:{exportId}`. Job state (`queued|processing|completed|
  failed|cancelled`, row counts, file path) is persisted in the
  `exports` Postgres table, so it survives process restarts and lets a
  reconnecting client see current status immediately.
- **Redis Pub/Sub** decouples the worker from the WebSocket layer: any
  number of worker processes and any number of WS-serving processes
  could scale independently, as long as they share the same Redis
  instance.
- **wsServer.js**: on connect, first queries Postgres for the export's
  current status and sends it immediately (so a reopened tab isn't
  stuck waiting for the next live tick), then subscribes the socket to
  the export's Redis channel. It also runs a server-side ping/pong
  heartbeat every 30s to detect and terminate stale connections, and
  handles `{"action":"cancel"}` client messages by setting a cancel flag
  in Redis that the worker checks between chunks.

## Prerequisites

- Docker and Docker Compose

## Setup & Run

```bash
# 1. Clone/enter the project
cd csv-export-service

# 2. Copy the env template (docker-compose already sets these for you,
#    this is mainly for running the app outside Docker)
cp .env.example .env

# 3. Build and start everything (app, Postgres, Redis)
docker-compose up --build

# 4. Wait for all three services to report healthy (~30-60s the first
#    time, while Postgres seeds 120,000 rows)
docker-compose ps
```

Once healthy, open **http://localhost:8080** for the dashboard.

## Manual verification

```bash
# Health check
curl http://localhost:8080/health

# Start an export
curl -X POST http://localhost:8080/api/exports
# => {"exportId":"..."}

# List recent exports
curl http://localhost:8080/api/exports

# Watch progress (requires a ws client, e.g. `npx wscat`)
npx wscat -c ws://localhost:8080/ws/exports/<exportId>

# Download once completed
curl -OJ http://localhost:8080/api/exports/<exportId>/download

# Cancel mid-export: send {"action":"cancel"} over the wscat connection
```

## Running without Docker (local dev)

```bash
npm install
# start your own local Postgres (seeded with seeds/001_init.sql) and Redis
export $(cat .env.example | xargs)
npm start
```

## Project layout

```
.
├── docker-compose.yml
├── Dockerfile
├── .env.example
├── package.json
├── seeds/
│   └── 001_init.sql        # users + exports tables, 120k seed rows
├── src/
│   ├── server.js           # Express app, HTTP routes, boot
│   ├── db.js                # pg Pool
│   ├── redis.js             # pub/sub + cancel-flag helpers
│   ├── exportWorker.js      # streaming export job
│   └── wsServer.js          # WebSocket upgrade + lifecycle + heartbeat
└── public/
    └── index.html           # vanilla-JS dashboard
```

## Notes / trade-offs

- CSV files are written to `/app/exports` inside the container (backed
  by the `exports_data` volume) rather than kept in memory, so exports
  scale past available RAM.
- Progress messages are sent once per 10,000-row chunk. At typical
  Postgres throughput this comfortably satisfies the "at least once
  every 2 seconds" requirement; lower `CHUNK_SIZE` in `.env` if your
  hardware processes chunks slower than that.
- Cancellation is cooperative: the worker checks a Redis flag between
  chunks rather than being forcibly killed, so a cancel takes effect at
  the next chunk boundary (well under the 2-second requirement in
  practice).
- WebSocket auth (JWT on handshake) is stubbed via `JWT_SECRET` in the
  env file but not enforced, per the spec's "not a primary focus" note.
  Wiring it in would mean validating a `token` query param during the
  `upgrade` event in `wsServer.js` before calling `handleUpgrade`.
