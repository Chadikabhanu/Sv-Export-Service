const fs = require('fs');
const path = require('path');
const { pool } = require('./db');
const { publishProgress, isCancelled } = require('./redis');

const CHUNK_SIZE = parseInt(process.env.CHUNK_SIZE || '10000', 10);
const EXPORTS_DIR = process.env.EXPORTS_DIR || path.join(__dirname, '..', 'exports');

if (!fs.existsSync(EXPORTS_DIR)) {
  fs.mkdirSync(EXPORTS_DIR, { recursive: true });
}

function csvEscape(value) {
  if (value === null || value === undefined) return '';
  const str = String(value);
  if (/[",\n]/.test(str)) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

async function updateJob(exportId, fields) {
  const keys = Object.keys(fields);
  const setClause = keys.map((k, i) => `${k} = $${i + 2}`).join(', ');
  await pool.query(
    `UPDATE exports SET ${setClause} WHERE export_id = $1`,
    [exportId, ...keys.map((k) => fields[k])]
  );
}

async function runExport(exportId) {
  const startedAt = Date.now();
  const filePath = path.join(EXPORTS_DIR, `export-${exportId}.csv`);
  const writeStream = fs.createWriteStream(filePath);

  try {
    const totalResult = await pool.query('SELECT COUNT(*)::int AS count FROM users');
    const total = totalResult.rows[0].count;

    await updateJob(exportId, { status: 'processing', total, started_at: new Date() });
    await publishProgress(exportId, {
      exportId,
      status: 'processing',
      progress: { total, processed: 0, percentage: 0, etaSeconds: null },
      timestamp: new Date().toISOString(),
    });

    writeStream.write('id,name,email,created_at\n');

    let processed = 0;
    let lastId = 0;
    let lastUpdateTime = Date.now();

    while (processed < total) {
      if (await isCancelled(exportId)) {
        writeStream.end();
        await updateJob(exportId, { status: 'cancelled' });
        await publishProgress(exportId, {
          exportId,
          status: 'cancelled',
          timestamp: new Date().toISOString(),
        });
        return;
      }

      // Keyset pagination (WHERE id > lastId) instead of OFFSET so performance
      // stays constant as we page deeper into a 100k+ row table.
      const { rows } = await pool.query(
        `SELECT id, name, email, created_at FROM users
         WHERE id > $1 ORDER BY id ASC LIMIT $2`,
        [lastId, CHUNK_SIZE]
      );

      if (rows.length === 0) break;

      for (const row of rows) {
        writeStream.write(
          `${row.id},${csvEscape(row.name)},${csvEscape(row.email)},${row.created_at.toISOString()}\n`
        );
      }

      lastId = rows[rows.length - 1].id;
      processed += rows.length;

      const now = Date.now();
      const elapsedSinceLast = (now - lastUpdateTime) / 1000;
      const elapsedTotal = (now - startedAt) / 1000;
      const speed = processed / elapsedTotal; // rows/sec average
      const remaining = total - processed;
      const etaSeconds = speed > 0 ? Math.round(remaining / speed) : null;
      lastUpdateTime = now;

      await updateJob(exportId, { processed });
      await publishProgress(exportId, {
        exportId,
        status: 'processing',
        progress: {
          total,
          processed,
          percentage: Math.round((processed / total) * 10000) / 100,
          etaSeconds,
        },
        timestamp: new Date().toISOString(),
      });

      void elapsedSinceLast; // reserved for future rate smoothing
    }

    await new Promise((resolve, reject) => {
      writeStream.end((err) => (err ? reject(err) : resolve()));
    });

    const stats = fs.statSync(filePath);
    const durationSeconds = Math.round(((Date.now() - startedAt) / 1000) * 100) / 100;

    await updateJob(exportId, {
      status: 'completed',
      file_path: filePath,
      file_size: stats.size,
      completed_at: new Date(),
    });

    await publishProgress(exportId, {
      exportId,
      status: 'completed',
      downloadUrl: `/api/exports/${exportId}/download`,
      fileSize: stats.size,
      durationSeconds,
    });
  } catch (err) {
    console.error(`[export ${exportId}] failed:`, err);
    try {
      writeStream.end();
    } catch (_) {
      /* ignore */
    }
    await updateJob(exportId, { status: 'failed', error: err.message }).catch(() => {});
    await publishProgress(exportId, {
      exportId,
      status: 'failed',
      error: err.message,
      timestamp: new Date().toISOString(),
    }).catch(() => {});
  }
}

module.exports = { runExport };
