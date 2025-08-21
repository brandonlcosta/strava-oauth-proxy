require('dotenv').config();
const express = require('express');
const axios = require('axios');
const { google } = require('googleapis');

const app = express();
const port = process.env.PORT || 3000;

// --- Middleware ---
app.use(express.json()); // needed for Strava's webhook POST (application/json)
app.use(express.urlencoded({ extended: true }));

// --- Tiny in-memory queue for webhook events ---
const inbox = [];
let processing = false;

async function drainQueue() {
  if (processing) return;
  processing = true;
  try {
    while (inbox.length) {
      const evt = inbox.shift();
      // Minimal: write raw payload to an "inbox" sheet for auditing.
      await appendToSheet('inbox', [[
        new Date().toISOString(),
        evt.object_type || '',
        evt.aspect_type || '',
        evt.object_id || '',
        evt.owner_id || '',
        JSON.stringify(evt || {})
      ]]);
    }
    console.log('[QUEUE] drained');
  } catch (err) {
    console.error('[QUEUE ERROR]', err?.response?.data || err.message || err);
  } finally {
    processing = false;
  }
}
setInterval(drainQueue, 3000);

// --- Home / health ---
app.get('/', (req, res) => {
  res.send(`<h1>SacUltraCrew OAuth + Webhook</h1>
  <p>Health: <a href="/health">/health</a></p>
  <p>Join: <a href="/join">/join</a></p>
  <p>Sheets ping: <a href="/debug/sheets-ping">/debug/sheets-ping</a></p>`);
});

app.get('/health', (req, res) => res.status(200).send('ok'));

// --- OAuth: start ---
app.get('/join', (req, res) => {
  const authUrl =
    `https://www.strava.com/oauth/authorize?` +
    `client_id=${process.env.STRAVA_CLIENT_ID}` +
    `&response_type=code` +
    `&redirect_uri=${encodeURIComponent(process.env.REDIRECT_URI)}` +
    `&scope=read,activity:read_all,profile:read_all` +
    `&approval_prompt=auto`;

  res.redirect(authUrl);
});

// --- OAuth: callback ---
app.get('/join-callback', async (req, res) => {
  const code = req.query.code;
  if (!code) return res.status(400).send('Missing code parameter.');

  try {
    const tokenRes = await axios.post('https://www.strava.com/oauth/token', {
      client_id: process.env.STRAVA_CLIENT_ID,
      client_secret: process.env.STRAVA_CLIENT_SECRET,
      code,
      grant_type: 'authorization_code',
    });

    const {
      access_token,
      refresh_token,
      expires_at,
      athlete: { id, firstname, lastname }
    } = tokenRes.data;

    await appendToSheet('athletes', [[
      id, `${firstname} ${lastname}`, access_token, refresh_token, expires_at
    ]]);

    res.send(`<h2>✅ You're connected, ${firstname}!</h2><p>You can close this tab.</p>`);
  } catch (err) {
    console.error('[ERROR] Strava token exchange failed:', err?.response?.data || err.message);
    res.status(500).send('OAuth failed.');
  }
});

// --- Webhook: verification (GET) ---
app.get('/webhook', (req, res) => {
  const mode = (req.query['hub.mode'] || '').trim();
  const provided = (req.query['hub.verify_token'] || '').trim();
  const challenge = req.query['hub.challenge'];

  const expected = (process.env.STRAVA_VERIFY_TOKEN || '').trim();
  const ok = mode === 'subscribe' && provided === expected && !!challenge;

  console.log('[VERIFY]', {
    mode,
    providedLen: provided.length,
    expectedLen: expected.length,
    match: provided === expected
  });

  if (ok) return res.status(200).json({ 'hub.challenge': challenge });
  return res.status(403).send('Verification failed.');
});

// --- Webhook: events (POST) ---
app.post('/webhook', async (req, res) => {
  try {
    // Immediately ack to Strava
    res.sendStatus(200);

    // Push into our queue, process later
    const body = req.body || {};
    console.log('[WEBHOOK]', body);
    inbox.push(body);

    // (Optional) If you want to fetch the activity on create, we can add it later.
  } catch (err) {
    console.error('[WEBHOOK ERROR]', err);
    // Still ack; do not cause retries
  }
});

// --- Debug: write a test row to inbox ---
app.get('/debug/sheets-ping', async (req, res) => {
  try {
    await appendToSheet('inbox', [[
      new Date().toISOString(), 'diag', 'ping', '', '', 'hello from Render'
    ]]);
    return res.status(200).send('OK: wrote a test row to inbox');
  } catch (e) {
    console.error('[DEBUG SHEETS PING ERROR]', e?.response?.data || e.message);
    return res.status(500).send('ERROR: ' + (e?.response?.data?.error?.message || e.message));
  }
});

// --- Debug: safe env diagnostics (no secrets leaked) ---
app.get('/debug/env', (req, res) => {
  try {
    const pvkLen = ((process.env.GOOGLE_PRIVATE_KEY || '').trim()).length;
    const tokenLen = ((process.env.STRAVA_VERIFY_TOKEN || '').trim()).length;
    const sheetId = (process.env.GOOGLE_SHEET_ID || '').slice(0, 6) + '...' + (process.env.GOOGLE_SHEET_ID || '').slice(-6);
    res.status(200).json({
      STRAVA_CLIENT_ID: process.env.STRAVA_CLIENT_ID || '(missing)',
      STRAVA_VERIFY_TOKEN_len: tokenLen,
      GOOGLE_PRIVATE_KEY_len: pvkLen,
      GOOGLE_SHEET_ID_masked: sheetId,
      REDIRECT_URI: process.env.REDIRECT_URI || '(missing)'
    });
  } catch (e) {
    res.status(500).send(e.message);
  }
});

// --- Google Sheets helper (auto-creates the tab if missing) ---
async function appendToSheet(tabName, values) {
  const sheets = google.sheets('v4');

  // Handle keys that were pasted with quotes around them
  let privateKey = process.env.GOOGLE_PRIVATE_KEY || '';
  if (privateKey.startsWith('"') && privateKey.endsWith('"')) {
    privateKey = privateKey.slice(1, -1);
  }
  privateKey = privateKey.replace(/\\n/g, '\n');

  const jwt = new google.auth.JWT({
    email: process.env.GOOGLE_SERVICE_EMAIL,
    key: privateKey,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  await jwt.authorize();

  const spreadsheetId = process.env.GOOGLE_SHEET_ID;

  // 1) Ensure the tab exists (create if missing)
  const meta = await sheets.spreadsheets.get({ auth: jwt, spreadsheetId });
  const titles = new Set((meta.data.sheets || []).map(s => s.properties.title));
  if (!titles.has(tabName)) {
    await sheets.spreadsheets.batchUpdate({
      auth: jwt,
      spreadsheetId,
      requestBody: {
        requests: [{ addSheet: { properties: { title: tabName } } }]
      }
    });
    // If it's the inbox, drop headers for sanity
    if (tabName === 'inbox') {
      await sheets.spreadsheets.values.update({
        auth: jwt,
        spreadsheetId,
        range: `${tabName}!A1:F1`,
        valueInputOption: 'RAW',
        requestBody: {
          values: [[
            'ts', 'object_type', 'aspect_type', 'object_id', 'owner_id', 'raw_json'
          ]]
        }
      });
    }
  }

  // 2) Append the row(s)
  await sheets.spreadsheets.values.append({
    auth: jwt,
    spreadsheetId,
    range: `${tabName}!A1`,
    valueInputOption: 'RAW',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values },
  });

  console.log(`[SHEETS] appended ${values.length} row(s) to ${tabName}`);
}

app.listen(port, () => {
  console.log(`Server listening on port ${port}`);
});
