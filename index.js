// index.js
require('dotenv').config();
const express = require('express');
const axios = require('axios');
const { google } = require('googleapis');

const app = express();
const port = process.env.PORT || 3000;

// ---------- Middleware ----------
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ---------- Small in-memory queue ----------
const inboxQ = [];
let queueBusy = false;

async function drainQueue() {
  if (queueBusy) return;
  queueBusy = true;
  try {
    while (inboxQ.length) {
      const evt = inboxQ.shift();

      // 1) Always write raw payload to "inbox" for auditing
      await appendToSheet('inbox', [[
        new Date().toISOString(),
        evt.object_type || '',
        evt.aspect_type || '',
        evt.object_id || '',
        evt.owner_id || '',
        JSON.stringify(evt || {})
      ]]);

      // 2) On brand-new activity, fetch + append to activities/segment_efforts
      if (evt.object_type === 'activity' && evt.aspect_type === 'create') {
        await processNewActivity(evt.object_id, evt.owner_id);
      }

      // 3) (Optional) handle delete/update here later if you want
    }
  } catch (err) {
    console.error('[QUEUE ERROR]', err?.response?.data || err.message || err);
  } finally {
    queueBusy = false;
  }
}
setInterval(drainQueue, 3000);

// ---------- Basic pages / health ----------
app.get('/', (req, res) => {
  res.send(`<h1>SacUltraCrew OAuth + Webhook</h1>
  <p>Health: <a href="/health">/health</a></p>
  <p>Join: <a href="/join">/join</a></p>
  <p>Sheets ping: <a href="/debug/sheets-ping">/debug/sheets-ping</a></p>`);
});
app.get('/health', (_req, res) => res.status(200).send('ok'));

// ---------- OAuth: start ----------
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

// ---------- OAuth: callback ----------
app.get('/join-callback', async (req, res) => {
  const code = req.query.code;
  if (!code) return res.status(400).send('Missing code parameter.');

  try {
    const { data } = await axios.post('https://www.strava.com/oauth/token', {
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
    } = data;

    await ensureTabWithHeaders('athletes', ['athlete_id','athlete_name','access_token','refresh_token','expires_at']);
    await appendRows('athletes', [[ id, `${firstname} ${lastname}`, access_token, refresh_token, expires_at ]]);

    res.send(`<h2>✅ You're connected, ${firstname}!</h2><p>You can close this tab.</p>`);
  } catch (err) {
    console.error('[OAUTH ERROR]', err?.response?.data || err.message);
    res.status(500).send('OAuth failed.');
  }
});

// ---------- Webhook verify (GET) ----------
app.get('/webhook', (req, res) => {
  const mode = (req.query['hub.mode'] || '').trim();
  const provided = (req.query['hub.verify_token'] || '').trim();
  const challenge = req.query['hub.challenge'];
  const expected = (process.env.STRAVA_VERIFY_TOKEN || '').trim();

  const ok = mode === 'subscribe' && provided === expected && !!challenge;
  console.log('[VERIFY]', { mode, providedLen: provided.length, expectedLen: expected.length, match: provided === expected });

  if (ok) return res.status(200).json({ 'hub.challenge': challenge });
  return res.status(403).send('Verification failed.');
});

// ---------- Webhook events (POST) ----------
app.post('/webhook', async (req, res) => {
  try {
    res.sendStatus(200); // ACK fast
    const evt = req.body || {};
    console.log('[WEBHOOK]', evt);
    inboxQ.push(evt);
        if (body.object_type === 'activity') {
      if (body.aspect_type === 'create') {
        processNewActivity(body.object_id, body.owner_id);
      } else if (body.aspect_type === 'update') {
        processActivityUpdate(body); // <-- add this line
      } else if (body.aspect_type === 'delete') {
        // optional: mark as deleted or handle later with SafeSync
        console.log('[DELETE] activity', body.object_id);
      }
    }

  } catch (err) {
    console.error('[WEBHOOK ERROR]', err);
    // still ACKed above to prevent retries
  }
});

// ---------- Debug routes ----------
app.get('/debug/sheets-ping', async (_req, res) => {
  try {
    await appendToSheet('inbox', [[ new Date().toISOString(), 'diag', 'ping', '', '', 'hello from Render' ]]);
    res.status(200).send('OK: wrote a test row to inbox');
  } catch (e) {
    console.error('[DEBUG SHEETS PING ERROR]', e?.response?.data || e.message);
    res.status(500).send('ERROR: ' + (e?.response?.data?.error?.message || e.message));
  }
});

app.get('/debug/env', (_req, res) => {
  const mask = (s) => s ? (s.slice(0,6) + '...' + s.slice(-6)) : '(missing)';
  res.status(200).json({
    STRAVA_CLIENT_ID: process.env.STRAVA_CLIENT_ID || '(missing)',
    STRAVA_VERIFY_TOKEN_len: (process.env.STRAVA_VERIFY_TOKEN || '').trim().length,
    GOOGLE_PRIVATE_KEY_len: (process.env.GOOGLE_PRIVATE_KEY || '').trim().length,
    GOOGLE_SHEET_ID_masked: mask(process.env.GOOGLE_SHEET_ID || ''),
    REDIRECT_URI: process.env.REDIRECT_URI || '(missing)'
  });
});

// ---------- Sheets: client + helpers ----------
async function getSheetsClient() {
  const sheets = google.sheets('v4');

  // Handle quotes + \n in private key
  let privateKey = process.env.GOOGLE_PRIVATE_KEY || '';
  if (privateKey.startsWith('"') && privateKey.endsWith('"')) privateKey = privateKey.slice(1, -1);
  privateKey = privateKey.replace(/\\n/g, '\n');

  const jwt = new google.auth.JWT({
    email: process.env.GOOGLE_SERVICE_EMAIL,
    key: privateKey,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  await jwt.authorize();
  return { sheets, jwt, spreadsheetId: process.env.GOOGLE_SHEET_ID };
}

async function ensureTabWithHeaders(tabName, headers) {
  const { sheets, jwt, spreadsheetId } = await getSheetsClient();
  const meta = await sheets.spreadsheets.get({ auth: jwt, spreadsheetId });
  const titles = new Set((meta.data.sheets || []).map(s => s.properties.title));
  if (!titles.has(tabName)) {
    await sheets.spreadsheets.batchUpdate({
      auth: jwt,
      spreadsheetId,
      requestBody: { requests: [{ addSheet: { properties: { title: tabName } } }] }
    });
    // write headers
    await sheets.spreadsheets.values.update({
      auth: jwt,
      spreadsheetId,
      range: `${tabName}!A1:${String.fromCharCode(64 + headers.length)}1`,
      valueInputOption: 'RAW',
      requestBody: { values: [headers] }
    });
  }
}

async function appendToSheet(tabName, values) {
  const { sheets, jwt, spreadsheetId } = await getSheetsClient();
  // If it's the first write ever, create the tab with default headers
  if (tabName === 'inbox') {
    await ensureTabWithHeaders('inbox', ['ts','object_type','aspect_type','object_id','owner_id','raw_json']);
  }
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

async function appendRows(tabName, rows) {
  const { sheets, jwt, spreadsheetId } = await getSheetsClient();
  await sheets.spreadsheets.values.append({
    auth: jwt,
    spreadsheetId,
    range: `${tabName}!A1`,
    valueInputOption: 'RAW',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values: rows },
  });
}

async function readColumnA(tabName) {
  const { sheets, jwt, spreadsheetId } = await getSheetsClient();
  const resp = await sheets.spreadsheets.values.get({
    auth: jwt,
    spreadsheetId,
    range: `${tabName}!A2:A`
  }).catch(() => ({ data: { values: [] } }));
  const rows = (resp.data.values || []).flat().map(v => String(v));
  return new Set(rows);
}

async function getAthleteTokenFromSheet(athleteId) {
  const { sheets, jwt, spreadsheetId } = await getSheetsClient();
  const resp = await sheets.spreadsheets.values.get({
    auth: jwt,
    spreadsheetId,
    range: 'athletes!A2:E' // A:id, B:name, C:access, D:refresh, E:expires_at
  }).catch(() => ({ data: { values: [] } }));
  const rows = resp.data.values || [];
  for (const r of rows) {
    if (String(r[0]) === String(athleteId)) {
      return { access_token: r[2], athlete_name: r[1] };
    }
  }
  return { access_token: null, athlete_name: null };
}

// ---------- Mapping (matches your GAS schema) ----------
const ACTIVITIES_HEADERS = [
  'activity_id','athlete_id','athlete_name','name','sport_type',
  'distance_m','moving_time_s','elapsed_time_s','total_elev_gain_m',
  'start_date','start_date_local','timezone','utc_offset_sec',
  'start_lat','start_lng','gear_id',
  'avg_speed_m_s','max_speed_m_s',
  'has_heartrate','avg_heartrate','max_heartrate','suffer_score',
  'kudos_count','comment_count','achievement_count',
  'visibility','is_trainer','is_commute',
  'device_name','map_polyline','created_at',
  'is_night_run','is_5k_plus','local_hour','week_start','month',
  'efforts_scanned'
];

const EFFORTS_HEADERS = [
  'activity_id','athlete_id','segment_id','segment_name',
  'elapsed_time_s','moving_time_s',
  'start_date','start_date_local',
  'pr_rank','kom_rank','distance_m',
  'average_grade','elev_high_m','elev_low_m',
  'segment_start_lat','segment_start_lng',
  'segment_end_lat','segment_end_lng',
  'week_start','month'
];

function mapActivityRow(athleteName, athleteId, a) {
  const dLocal = new Date(a.start_date_local);
  const localHour = dLocal.getHours();
  const isNight = localHour >= 22;
  const is5k = (a.distance || 0) >= 5000;

  const weekStart = new Date(dLocal);
  weekStart.setDate(dLocal.getDate() - dLocal.getDay());
  weekStart.setHours(0,0,0,0);
  const month = `${dLocal.getFullYear()}-${String(dLocal.getMonth()+1).padStart(2,'0')}`;

  return [
    a.id,
    athleteId,
    athleteName || '',
    a.name || '',
    a.sport_type || '',
    a.distance || '',
    a.moving_time || '',
    a.elapsed_time || '',
    a.total_elevation_gain || '',
    a.start_date || '',
    a.start_date_local || '',
    a.timezone || '',
    a.utc_offset || '',
    (a.start_latlng && a.start_latlng[0]) || '',
    (a.start_latlng && a.start_latlng[1]) || '',
    a.gear_id || '',
    a.average_speed || '',
    a.max_speed || '',
    !!a.has_heartrate,
    a.average_heartrate || '',
    a.max_heartrate || '',
    a.suffer_score || '',
    a.kudos_count || '',
    a.comment_count || '',
    a.achievement_count || '',
    a.visibility || '',
    a.trainer || '',
    a.commute || '',
    a.device_name || '',
    (a.map && a.map.summary_polyline) || '',
    a.created_at || '',
    isNight,
    is5k,
    localHour,
    weekStart.toISOString().split('T')[0],
    month,
    true // efforts_scanned (webhook path already scanned)
  ];
}

function mapEffortRow(athleteId, act, e) {
  const d = new Date(e.start_date_local);
  const weekStart = new Date(d);
  weekStart.setDate(d.getDate() - d.getDay());
  weekStart.setHours(0,0,0,0);
  const month = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}`;
  const s = e.segment || {};
  return [
    act.id,
    athleteId,
    s.id || '',
    s.name || '',
    e.elapsed_time || '',
    e.moving_time || '',
    e.start_date || '',
    e.start_date_local || '',
    e.pr_rank || '',
    e.kom_rank || '',
    s.distance || '',
    s.average_grade || '',
    s.elevation_high || '',
    s.elevation_low || '',
    (s.start_latlng && s.start_latlng[0]) || '',
    (s.start_latlng && s.start_latlng[1]) || '',
    (s.end_latlng && s.end_latlng[0]) || '',
    (s.end_latlng && s.end_latlng[1]) || '',
    weekStart.toISOString().split('T')[0],
    month
  ];
}

// ---------- Process a "create" event: fetch + append ----------
async function processNewActivity(objectId, ownerId) {
  try {
    await ensureTabWithHeaders('activities', ACTIVITIES_HEADERS);
    await ensureTabWithHeaders('segment_efforts', EFFORTS_HEADERS);

    // de-dupe by activity_id in activities!A
    const existing = await readColumnA('activities');
    if (existing.has(String(objectId))) {
      console.log('[NEW ACT] duplicate, skipping', objectId);
      return;
    }

    const { access_token, athlete_name } = await getAthleteTokenFromSheet(ownerId);
    if (!access_token) {
      console.warn('[NEW ACT] no access_token for owner', ownerId);
      return;
    }

    const act = await axios.get(
      `https://www.strava.com/api/v3/activities/${objectId}?include_all_efforts=true`,
      { headers: { Authorization: `Bearer ${access_token}` } }
    ).then(r => r.data);

    const activityRow = mapActivityRow(athlete_name, ownerId, act);
    await appendRows('activities', [activityRow]);

    const efforts = (act.segment_efforts || []);
    if (efforts.length) {
      const effortRows = efforts.map(e => mapEffortRow(ownerId, act, e));
      await appendRows('segment_efforts', effortRows);
    }

    console.log('[NEW ACT] appended activity', objectId, 'efforts:', efforts.length);
  } catch (err) {
    console.error('[NEW ACT ERROR]', err?.response?.data || err.message || err);
  }
}

// Convert numeric column index to A1 letter(s) (1->A, 27->AA)
function colToA1(n) {
  let s = '';
  while (n > 0) {
    const m = (n - 1) % 26;
    s = String.fromCharCode(65 + m) + s;
    n = Math.floor((n - 1) / 26);
  }
  return s;
}

// Find the row index (2-based, skipping header) for a given activity_id in "activities"
async function findActivityRowIndex(activityId) {
  const { sheets, jwt, spreadsheetId } = await getSheetsClient();
  // Read id column A (skip header)
  const resp = await sheets.spreadsheets.values.get({
    auth: jwt,
    spreadsheetId,
    range: 'activities!A2:A'
  }).catch(() => ({ data: { values: [] } }));
  const vals = resp.data.values || [];
  for (let i = 0; i < vals.length; i++) {
    if (String(vals[i][0]) === String(activityId)) {
      return i + 2; // row number in sheet
    }
  }
  return null;
}

// Update an entire row in activities (overwrite the row with mapped values)
async function overwriteActivityRow(rowIndex, rowValues) {
  const { sheets, jwt, spreadsheetId } = await getSheetsClient();
  const colCount = ACTIVITIES_HEADERS.length;
  const endCol = colToA1(colCount);
  const range = `activities!A${rowIndex}:${endCol}${rowIndex}`;
  await sheets.spreadsheets.values.update({
    auth: jwt,
    spreadsheetId,
    range,
    valueInputOption: 'RAW',
    requestBody: { values: [rowValues] }
  });
}

// Handle aspect_type === 'update'
async function processActivityUpdate(evt) {
  try {
    const { object_id, owner_id } = evt;
    await ensureTabWithHeaders('activities', ACTIVITIES_HEADERS);

    // 1) get athlete token
    const { access_token, athlete_name } = await getAthleteTokenFromSheet(owner_id);
    if (!access_token) {
      console.warn('[UPDATE] no access_token for owner', owner_id);
      return;
    }

    // 2) fetch current activity (include efforts if you want to refresh those too)
    const act = await axios.get(
      `https://www.strava.com/api/v3/activities/${object_id}?include_all_efforts=false`,
      { headers: { Authorization: `Bearer ${access_token}` } }
    ).then(r => r.data);

    // 3) map row in your schema
    const row = mapActivityRow(athlete_name, owner_id, act);

    // 4) locate row; if missing, append (maybe create occurred when service was asleep)
    const rowIndex = await findActivityRowIndex(object_id);
    if (!rowIndex) {
      console.log('[UPDATE] activity not found, appending instead', object_id);
      await appendRows('activities', [row]);
      return;
    }

    // 5) overwrite existing row
    await overwriteActivityRow(rowIndex, row);
    console.log('[UPDATE] refreshed activity row', object_id, 'at row', rowIndex);
  } catch (err) {
    console.error('[UPDATE ERROR]', err?.response?.data || err.message || err);
  }
}


// ---------- Start server ----------
app.listen(port, () => {
  console.log(`Server listening on port ${port}`);
});
