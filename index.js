// index.js — SacUltraCrew OAuth + Webhooks (Render)
// -------------------------------------------------
require('dotenv').config();

const express = require('express');
const axios = require('axios');
const { google } = require('googleapis');

const app = express();
const port = process.env.PORT || 3000;

// ================================
// Basic middleware
// ================================
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ================================
// Small in-memory queue
// ================================
const inboxQ = [];
let queueBusy = false;

setInterval(drainQueue, 3000);

async function drainQueue() {
  if (queueBusy) return;
  queueBusy = true;
  try {
    while (inboxQ.length) {
      const evt = inboxQ.shift();

      // 1) log every event to inbox
      await appendToSheet('inbox', [[
        new Date().toISOString(),
        evt.object_type || '',
        evt.aspect_type || '',
        evt.object_id || '',
        evt.owner_id || '',
        JSON.stringify(evt || {})
      ]]);

      // 2) route by aspect
      if (evt.object_type === 'activity') {
        if (evt.aspect_type === 'create') {
          await processNewActivity(evt.object_id, evt.owner_id);
        } else if (evt.aspect_type === 'update') {
          await processActivityUpdate(evt);
        } else if (evt.aspect_type === 'delete') {
          // optional: mark deleted, or let a periodic SafeSync handle it
          console.log('[DELETE]', evt.object_id);
        }
      }
    }
  } catch (err) {
    console.error('[QUEUE ERROR]', err?.response?.data || err.message || err);
  } finally {
    queueBusy = false;
  }
}

// ================================
// Health + tiny landing
// ================================
app.get('/', (_req, res) => {
  res.status(200).send(
`<h1>SacUltraCrew OAuth + Webhook</h1>
<p>Health: <a href="/health">/health</a></p>
<p>Join: <a href="/join">/join</a></p>
<p>Sheets ping: <a href="/debug/sheets-ping">/debug/sheets-ping</a></p>`
  );
});
app.get('/health', (_req, res) => res.status(200).send('ok'));

// ================================
// OAuth: start
// ================================
app.get('/join', (_req, res) => {
  const url =
    `https://www.strava.com/oauth/authorize?` +
    `client_id=${process.env.STRAVA_CLIENT_ID}` +
    `&response_type=code` +
    `&redirect_uri=${encodeURIComponent(process.env.REDIRECT_URI)}` +
    `&scope=read,activity:read_all,profile:read_all` +
    `&approval_prompt=auto`;
  res.redirect(url);
});

// ================================
// OAuth: callback
// ================================
app.get('/join-callback', async (req, res) => {
  const code = req.query.code;
  if (!code) return res.status(400).send('Missing code');

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
    await upsertAthleteRow({
      athlete_id: id,
      athlete_name: `${firstname} ${lastname}`,
      access_token,
      refresh_token,
      expires_at
    });

    res.status(200).send(`<h2>✅ You're connected, ${firstname}!</h2><p>You can close this tab.</p>`);
  } catch (err) {
    console.error('[OAUTH ERROR]', err?.response?.data || err.message);
    res.status(500).send('OAuth failed.');
  }
});

// ================================
// Webhook: verify (GET)
// ================================
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

// ================================
// Webhook: events (POST)
// ================================
app.post('/webhook', async (req, res) => {
  try {
    // ACK immediately to stop Strava retries
    res.sendStatus(200);

    const evt = req.body || {};
    console.log('[WEBHOOK]', evt);
    inboxQ.push(evt);
  } catch (err) {
    console.error('[WEBHOOK ERROR]', err);
    // we already ACKed
  }
});

// ================================
// Debug routes
// ================================
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
    GOOGLE_SERVICE_EMAIL: process.env.GOOGLE_SERVICE_EMAIL || '(missing)',
    REDIRECT_URI: process.env.REDIRECT_URI || '(missing)'
  });
});

// ================================
// Google Sheets client + helpers
// ================================
async function getSheetsClient() {
  const sheets = google.sheets('v4');

  // Handle quotes + \n in private key from env
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
    await sheets.spreadsheets.values.update({
      auth: jwt,
      spreadsheetId,
      range: `${tabName}!A1:${colToA1(headers.length)}1`,
      valueInputOption: 'RAW',
      requestBody: { values: [headers] }
    });
  }
}

async function appendToSheet(tabName, values) {
  const { sheets, jwt, spreadsheetId } = await getSheetsClient();
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

// Upsert athlete row by athlete_id (keeps sheet tidy)
async function upsertAthleteRow({ athlete_id, athlete_name, access_token, refresh_token, expires_at }) {
  const { sheets, jwt, spreadsheetId } = await getSheetsClient();
  await ensureTabWithHeaders('athletes', ['athlete_id','athlete_name','access_token','refresh_token','expires_at']);

  const resp = await sheets.spreadsheets.values.get({
    auth: jwt,
    spreadsheetId,
    range: 'athletes!A2:E',
  }).catch(() => ({ data: { values: [] } }));

  const rows = resp.data.values || [];
  let foundIndex = -1;
  for (let i = 0; i < rows.length; i++) {
    if (String(rows[i][0]) === String(athlete_id)) { foundIndex = i; break; }
  }

  if (foundIndex === -1) {
    await appendRows('athletes', [[
      athlete_id, athlete_name, access_token, refresh_token, expires_at
    ]]);
  } else {
    const rowIndex = foundIndex + 2; // sheet row
    await sheets.spreadsheets.values.update({
      auth: jwt,
      spreadsheetId,
      range: `athletes!A${rowIndex}:E${rowIndex}`,
      valueInputOption: 'RAW',
      requestBody: { values: [[ athlete_id, athlete_name, access_token, refresh_token, expires_at ]] }
    });
  }
}

// Read first column (A) as Set of strings, skip header
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

// Read header row and return index (0-based) of a header name
async function getHeaderIndex(tabName, headerName) {
  const { sheets, jwt, spreadsheetId } = await getSheetsClient();
  const resp = await sheets.spreadsheets.values.get({
    auth: jwt,
    spreadsheetId,
    range: `${tabName}!1:1`
  });
  const headers = (resp.data.values && resp.data.values[0]) || [];
  return headers.findIndex(h => String(h).trim() === String(headerName).trim());
}

// Update a single cell by row index (1-based) and col index (0-based)
async function updateSingleCell(tabName, rowIndex1, colIndex0, value) {
  const { sheets, jwt, spreadsheetId } = await getSheetsClient();
  const colA1 = colToA1(colIndex0 + 1);
  const range = `${tabName}!${colA1}${rowIndex1}:${colA1}${rowIndex1}`;
  await sheets.spreadsheets.values.update({
    auth: jwt,
    spreadsheetId,
    range,
    valueInputOption: 'RAW',
    requestBody: { values: [[value]] }
  });
}


// ================================
// Auth helpers (refresh tokens)
// ================================
async function getAthleteAuth(athleteId) {
  const { sheets, jwt, spreadsheetId } = await getSheetsClient();
  const resp = await sheets.spreadsheets.values.get({
    auth: jwt,
    spreadsheetId,
    range: 'athletes!A2:E' // A:id, B:name, C:access, D:refresh, E:expires_at
  }).catch(() => ({ data: { values: [] } }));

  const rows = resp.data.values || [];
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    if (String(r[0]) === String(athleteId)) {
      return {
        rowIndex: i + 2,
        athlete_name: r[1] || '',
        access_token: r[2] || '',
        refresh_token: r[3] || '',
        expires_at: Number(r[4] || 0)
      };
    }
  }
  return null;
}

async function writeAthleteTokens(rowIndex, access_token, refresh_token, expires_at) {
  const { sheets, jwt, spreadsheetId } = await getSheetsClient();
  await sheets.spreadsheets.values.update({
    auth: jwt,
    spreadsheetId,
    range: `athletes!C${rowIndex}:E${rowIndex}`, // C:access, D:refresh, E:expires_at
    valueInputOption: 'RAW',
    requestBody: { values: [[access_token, refresh_token, expires_at]] }
  });
}

async function ensureFreshAccessToken(athleteId) {
  const auth = await getAthleteAuth(athleteId);
  if (!auth) {
    console.warn('[AUTH] No athlete row for', athleteId);
    return { access_token: null, athlete_name: null };
  }

  const now = Math.floor(Date.now() / 1000);
  if (auth.access_token && auth.expires_at && auth.expires_at - 60 > now) {
    return { access_token: auth.access_token, athlete_name: auth.athlete_name };
  }

  // refresh
  try {
    const { data } = await axios.post('https://www.strava.com/oauth/token', {
      client_id: process.env.STRAVA_CLIENT_ID,
      client_secret: process.env.STRAVA_CLIENT_SECRET,
      grant_type: 'refresh_token',
      refresh_token: auth.refresh_token
    });

    await writeAthleteTokens(auth.rowIndex, data.access_token, data.refresh_token, data.expires_at);
    return { access_token: data.access_token, athlete_name: auth.athlete_name };
  } catch (e) {
    console.error('[TOKEN REFRESH ERROR]', e?.response?.data || e.message);
    return { access_token: null, athlete_name: auth.athlete_name };
  }
}

// ================================
// Sheet schema + mappers
// ================================
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
    true // efforts scanned in webhook path
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

// ================================
// Activity processors
// ================================
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

    const { access_token, athlete_name } = await ensureFreshAccessToken(ownerId);
    if (!access_token) {
      console.warn('[NEW ACT] no usable access_token for owner', ownerId);
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

async function processActivityUpdate(evt) {
  try {
    const { object_id, owner_id, updates = {} } = evt;
    await ensureTabWithHeaders('activities', ACTIVITIES_HEADERS);

    // Find the row for this activity; if not found, fetch and append once
    let rowIndex = await findActivityRowIndex(object_id);
    if (!rowIndex) {
      console.log('[UPDATE] activity not found, calling processNewActivity first', object_id);
      await processNewActivity(object_id, owner_id);
      rowIndex = await findActivityRowIndex(object_id);
      if (!rowIndex) {
        console.warn('[UPDATE] still no row after fetch, giving up for now', object_id);
        return;
      }
    }

    // 1) FAST PATH: apply known updates directly from webhook payload (no Strava fetch)
    // Strava sends e.g. updates: { title: "New name" } for rename events
    const hasDirectChange =
      typeof updates.title !== 'undefined' ||
      typeof updates.visibility !== 'undefined' ||
      typeof updates.private !== 'undefined' || // older field
      typeof updates.type !== 'undefined' ||
      typeof updates.sport_type !== 'undefined';

    if (hasDirectChange) {
      // Map webhook fields -> our sheet columns
      // activities headers: 'name' is the title column
      const nameCol = await getHeaderIndex('activities', 'name');
      const visCol  = await getHeaderIndex('activities', 'visibility');
      const sportCol = await getHeaderIndex('activities', 'sport_type');

      // Title change
      if (typeof updates.title !== 'undefined' && nameCol >= 0) {
        await updateSingleCell('activities', rowIndex, nameCol, updates.title);
        console.log('[UPDATE] applied title from webhook for', object_id, '->', updates.title);
      }

      // Visibility change (Strava may send 'visibility' or 'private'; if private=true, set 'followers_only' or similar if you prefer)
      if (typeof updates.visibility !== 'undefined' && visCol >= 0) {
        await updateSingleCell('activities', rowIndex, visCol, updates.visibility);
        console.log('[UPDATE] applied visibility from webhook for', object_id, '->', updates.visibility);
      } else if (typeof updates.private !== 'undefined' && visCol >= 0) {
        // mirror older "private" into visibility, choose a convention
        const v = updates.private ? 'only_me' : 'everyone';
        await updateSingleCell('activities', rowIndex, visCol, v);
        console.log('[UPDATE] applied private->visibility for', object_id, '->', v);
      }

      // Sport type change
      if (typeof updates.type !== 'undefined' && sportCol >= 0) {
        await updateSingleCell('activities', rowIndex, sportCol, updates.type);
        console.log('[UPDATE] applied type from webhook for', object_id, '->', updates.type);
      } else if (typeof updates.sport_type !== 'undefined' && sportCol >= 0) {
        await updateSingleCell('activities', rowIndex, sportCol, updates.sport_type);
        console.log('[UPDATE] applied sport_type from webhook for', object_id, '->', updates.sport_type);
      }

      // Done with direct changes; you can bail here if you want.
      // If you ALSO want to refresh other fields from Strava, fall through to the fetch below.
    }

    // 2) SLOW PATH (optional): fetch latest and overwrite the row
    // This keeps the row fully in sync when changes aren't provided in 'updates'.
    const { access_token, athlete_name } = await ensureFreshAccessToken(owner_id);
    if (!access_token) {
      console.warn('[UPDATE] no usable access_token for owner', owner_id);
      return;
    }

    const act = await axios.get(
      `https://www.strava.com/api/v3/activities/${object_id}?include_all_efforts=false`,
      { headers: { Authorization: `Bearer ${access_token}` } }
    ).then(r => r.data);

    const fullRow = mapActivityRow(athlete_name, owner_id, act);
    await overwriteActivityRow(rowIndex, fullRow);
    console.log('[UPDATE] refreshed full activity row for', object_id, 'at row', rowIndex);

  } catch (err) {
    console.error('[UPDATE ERROR]', err?.response?.data || err.message || err);
  }
}


// ================================
// Sheet row locate/overwrite
// ================================
function colToA1(n) {
  let s = '';
  while (n > 0) {
    const m = (n - 1) % 26;
    s = String.fromCharCode(65 + m) + s;
    n = Math.floor((n - 1) / 26);
  }
  return s;
}

async function findActivityRowIndex(activityId) {
  const { sheets, jwt, spreadsheetId } = await getSheetsClient();
  const resp = await sheets.spreadsheets.values.get({
    auth: jwt,
    spreadsheetId,
    range: 'activities!A2:A'
  }).catch(() => ({ data: { values: [] } }));

  const vals = resp.data.values || [];
  for (let i = 0; i < vals.length; i++) {
    if (String(vals[i][0]) === String(activityId)) {
      return i + 2; // actual sheet row
    }
  }
  return null;
}

async function overwriteActivityRow(rowIndex, rowValues) {
  const { sheets, jwt, spreadsheetId } = await getSheetsClient();
  const endCol = colToA1(ACTIVITIES_HEADERS.length);
  await sheets.spreadsheets.values.update({
    auth: jwt,
    spreadsheetId,
    range: `activities!A${rowIndex}:${endCol}${rowIndex}`,
    valueInputOption: 'RAW',
    requestBody: { values: [rowValues] }
  });
}

// ================================
// Boot
// ================================
app.listen(port, () => {
  console.log(`Server listening on port ${port}`);
});
