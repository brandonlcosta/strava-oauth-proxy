require('dotenv').config();
const express = require('express');
const axios = require('axios');
const { google } = require('googleapis');

const app = express();
const port = process.env.PORT || 3000;

app.get('/', (req, res) => {
  res.send(`<h1>SacultraCrew OAuth Proxy</h1><p>Go to <a href="/join">/join</a> to authorize Strava</p>`);
});

app.get('/join', (req, res) => {
  const authUrl = `https://www.strava.com/oauth/authorize?client_id=${process.env.STRAVA_CLIENT_ID}&response_type=code&redirect_uri=${process.env.STRAVA_REDIRECT_URI}&scope=read,activity:read_all,profile:read_all&approval_prompt=auto`;
  res.redirect(authUrl);
});

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

    await appendToSheet([id, `${firstname} ${lastname}`, access_token, refresh_token, expires_at]);

    res.send(`<h2>✅ You're connected, ${firstname}!</h2><p>You can now close this tab.</p>`);
  } catch (err) {
    console.error('[ERROR] Strava token exchange failed:', err);
    res.status(500).send('OAuth failed.');
  }
});

async function appendToSheet(data) {
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
    range: 'athletes!A1',
    valueInputOption: 'RAW',
    insertDataOption: 'INSERT_ROWS',
    requestBody: {
      values: [data],
    },
  });
}

app.listen(port, () => {
  console.log(`Server listening at http://localhost:${port}`);
});
