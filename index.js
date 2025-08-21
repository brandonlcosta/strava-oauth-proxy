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
      // You can later upgrade this to kick a fetch of the activity, etc.
      await appendToSheet('inbox', [[
        new Date().toISOString(),
        evt.object_type,
        evt.aspect_type,
        evt.object_id,
        evt.owner_id,
        JSON.stringify(evt)
      ]]);
    }
  } catch (err) {
    console.error('[QUEUE ERROR]', err);
  } finally {
    processing = false;
  }
}
setInterval(drainQueue, 3000);

// --- Home / health ---
app.get('/', (req, res) => {
  res.send(`<h1>SacUltraCrew OAuth + Webhook</h1>
  <p>Health: <a href="/health">/health</a></p>
  <p>Join: <a href="/join">/join</a></p>`);
});

app.get('/health', (req, res) => res.status(200).send('ok'));

// --- OAuth: start ---
app.get('/join', (req, res) => {
  const authUrl =
    `https://www.strava.com/oauth/authorize?` +
    `client_id=${process.env.STRAVA_CLIENT_ID}` +
    `&response_type=code` +
    `&redirect_uri=${encodeURIComponent(process.env.REDIRECT_URI)}` + // <= from .env
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
  const { 'hub.mode': mode, 'hub.verify_token': token, 'hub.challenge': challenge } = req.query;
  if (mode === 'subscribe' && token === process.env.STRAVA_VERIFY_TOKEN && challenge) {
    return res.status(200).json({ 'hub.challenge': challenge });
  }
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

    // (Optional) If you want to fetch the activity on create:
    // if (body.object_type === 'activity' && body.aspect_type === 'create') { ... }
  } catch (err) {
    console.error('[WEBHOOK ERROR]', err);
    // Still ack; do not cause retries
  }
});

// --- Google Sheets helper ---
async function appendToSheet(tabName, values) {
  const sheets = google.sheets('v4');
  const jwt = new google.auth.JWT({
    email: process.env.GOOGLE_SERVICE_EMAIL,
    key: process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n'),
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  await jwt.authorize();

  await sheets.spreadsheets.values.append({
    auth: jwt,
    spreadsheetId: process.env.GOOGLE_SHEET_ID,
    range: `${tabName}!A1`,
    valueInputOption: 'RAW',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values },
  });
}

app.listen(port, () => {
  console.log(`Server listening on port ${port}`);
});

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
