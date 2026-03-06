const express = require("express");
const cors = require("cors");
const admin = require("firebase-admin");
const fetch = require("node-fetch");

admin.initializeApp({
  credential: admin.credential.cert(JSON.parse(process.env.SERVICE_ACCOUNT_KEY)),
});

const db = admin.firestore();

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

/* =========================
   PRESETS
========================= */

const NO_LEAD_PRESETS = {
  finish: {
    title: "⚠️ Completa tu solicitud",
    body: "Iniciaste una solicitud pero no la terminaste. Complétala ahora.",
  },
  dont_miss: {
    title: "⏳ No la dejes pasar",
    body: "Tu solicitud aún no está completa. Toma solo 2 minutos.",
  },
  almost: {
    title: "🚀 Casi listo",
    body: "Estás a un paso de completar tu solicitud.",
  },
};

const DAY_PRESETS = {
  1: "📄 Solicitud recibida",
  2: "🔍 Verificación en proceso",
  3: "📊 Revisión en curso",
  4: "🧩 Analizando opciones",
  5: "✅ Revisión final",
  6: "⏱ Casi listo",
  7: "🎉 Proceso completado",
};

/* =========================
   HELPERS
========================= */

function calcDayFromCreatedAt(createdAt) {
  if (!createdAt) return null;
  const start = createdAt.toDate();
  const now = new Date();
  const day = Math.ceil((now - start) / 86400000);
  return Math.min(7, Math.max(1, day));
}

async function sendPush(tokens, payload, screen) {
  let sent = 0;
  const invalidTokens = [];

  for (let i = 0; i < tokens.length; i += 100) {
    const chunk = tokens.slice(i, i + 100);

    const resp = await fetch("https://exp.host/--/api/v2/push/send", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(
        chunk.map((to) => ({
          to,
          sound: "default",
          title: payload.title,
          body: payload.body,
          data: { screen },
        }))
      ),
    });

    const json = await resp.json();
    if (!resp.ok) throw new Error(JSON.stringify(json));

    json.data.forEach((result, idx) => {
      if (result.status === "error") {
        const errorType = result.details?.error;
        if (
          errorType === "DeviceNotRegistered" ||
          errorType === "InvalidCredentials"
        ) {
          invalidTokens.push(chunk[idx]);
        }
      } else if (result.status === "ok") {
        sent++;
      }
    });
  }

  return { sent, invalidTokens };
}

async function cleanInvalidTokens(invalidTokens) {
  if (invalidTokens.length === 0) return 0;

  const batch = db.batch();
  let cleaned = 0;

  const devicesSnap = await db.collection("devices").get();
  devicesSnap.docs.forEach((doc) => {
    const token = doc.data().expo_push_token;
    if (invalidTokens.includes(token)) {
      batch.delete(doc.ref);
      cleaned++;
    }
  });

  const leadsSnap = await db.collection("leads").get();
  leadsSnap.docs.forEach((doc) => {
    const token = doc.data().pushToken;
    if (invalidTokens.includes(token)) {
      batch.update(doc.ref, { pushToken: admin.firestore.FieldValue.delete() });
      cleaned++;
    }
  });

  await batch.commit();
  return cleaned;
}

/* =========================
   CONTROL CENTER (UI)
========================= */

app.get("/", async (req, res) => {
  const { msg, error } = req.query;

  const devicesSnap = await db.collection("devices").get();
  const leadsSnap = await db.collection("leads").get();

  const stats = {
    noLead: 0,
    days: { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 7: 0 },
  };

  devicesSnap.docs.forEach((d) => {
    if (d.data().hasLead !== true) stats.noLead++;
  });

  leadsSnap.docs.forEach((d) => {
    const day = calcDayFromCreatedAt(d.data().createdAt);
    if (day) stats.days[day]++;
  });

  const totalLeads = Object.values(stats.days).reduce((a, b) => a + b, 0);

  res.send(`
<!DOCTYPE html>
<html>
<head>
<title>Push Control Center</title>
<style>
body{font-family:Arial;background:#f5f7fb;padding:30px}
.container{max-width:1200px;margin:auto}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:20px}
.card{background:#fff;padding:20px;border-radius:14px;box-shadow:0 8px 20px rgba(0,0,0,.08)}
button{width:100%;padding:14px;font-size:16px;font-weight:700;border-radius:10px;border:none;margin-top:8px;cursor:pointer}
.primary{background:#1FA971;color:#fff}
.warn{background:#F6A800;color:#fff}
.danger{background:#dc3545;color:#fff}
.alldays{background:#6610f2;color:#fff;font-size:18px;padding:18px;margin-top:16px}
input,textarea,select{width:100%;padding:12px;font-size:15px;border-radius:8px;border:1px solid #ccc;margin-top:6px}
textarea{min-height:90px}
.ok{background:#e7f9ed;color:#1e7e34;padding:12px;border-radius:8px;margin-bottom:20px}
.err{background:#fdecea;color:#b21f2d;padding:12px;border-radius:8px;margin-bottom:20px}
.stat{font-weight:700}
.divider{border:none;border-top:2px solid #eee;margin:16px 0}
</style>
</head>
<body>

<div class="container">
<h1>📣 Push Control Center</h1>

${msg ? `<div class="ok">${msg}</div>` : ""}
${error ? `<div class="err">${error}</div>` : ""}

<div class="grid">

<div class="card">
<h2>Usuarios sin solicitud</h2>
<form method="POST" action="/preview">
<input type="hidden" name="type" value="no-lead"/>
<button name="preset" value="finish" class="warn">⚠️ Completar solicitud</button>
<button name="preset" value="dont_miss" class="warn">⏳ No la dejes pasar</button>
<button name="preset" value="almost" class="warn">🚀 Casi listo</button>
</form>
<p class="stat">Sin solicitud: ${stats.noLead}</p>
</div>

<div class="card">
<h2>Solicitudes (día 1–7)</h2>
<form method="POST" action="/preview">
<input type="hidden" name="type" value="day"/>
${Object.keys(DAY_PRESETS)
  .map((d) => `<button name="day" value="${d}" class="primary">Día ${d}</button>`)
  .join("")}
</form>

<hr class="divider"/>

<!-- ✅ NUEVO: Botón enviar todos los días de una sola vez -->
<form method="POST" action="/preview-all-days">
  <button class="alldays" onclick="return confirm('¿Ver preview para enviar a TODOS los leads (días 1–7)?')">
    🚀 Enviar Todos los Días (1–7)
  </button>
</form>

<hr class="divider"/>

${Object.entries(stats.days)
  .map(([d, v]) => `<p class="stat">Día ${d}: ${v}</p>`)
  .join("")}
<p class="stat" style="color:#6610f2;border-top:2px solid #eee;margin-top:8px;padding-top:8px">Total leads: ${totalLeads}</p>
</div>

<div class="card">
<h2>Envío manual</h2>
<form method="POST" action="/preview">
<label>Título</label>
<input name="manualTitle" required />
<label>Mensaje</label>
<textarea name="manualBody" required></textarea>
<label>Audiencia</label>
<select name="type">
  <option value="all">Todos</option>
  <option value="no-lead">Sin solicitud</option>
  <option value="lead">Con solicitud</option>
</select>
<button class="primary">Preview</button>
</form>
</div>

<div class="card">
<h2>🗑️ Mantenimiento</h2>
<p>Limpia tokens de dispositivos que desinstalaron la app</p>
<form method="GET" action="/clean-tokens">
<button class="danger" onclick="return confirm('¿Limpiar tokens muertos?')">Limpiar tokens inválidos</button>
</form>
<p style="margin-top:15px">Analiza duplicados y problemas en la base de datos</p>
<form method="GET" action="/diagnostico">
<button class="primary">🔍 Ver Diagnóstico</button>
</form>
</div>

</div>
</div>
</body>
</html>
`);
});

/* =========================
   PREVIEW ALL DAYS  ← NUEVO
========================= */

app.post("/preview-all-days", async (req, res) => {
  const leadsSnap = await db.collection("leads").get();
  const dayCounts = { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 7: 0 };

  leadsSnap.docs.forEach((d) => {
    const day = calcDayFromCreatedAt(d.data().createdAt);
    if (day) dayCounts[day]++;
  });

  const total = Object.values(dayCounts).reduce((a, b) => a + b, 0);

  const rows = Object.entries(DAY_PRESETS)
    .map(
      ([d, title]) => `
      <tr>
        <td style="padding:10px;border-bottom:1px solid #eee"><strong>Día ${d}</strong></td>
        <td style="padding:10px;border-bottom:1px solid #eee">${title}</td>
        <td style="padding:10px;border-bottom:1px solid #eee;color:#1FA971;font-weight:700">${dayCounts[d]} usuarios</td>
      </tr>`
    )
    .join("");

  res.send(`
<!DOCTYPE html>
<html>
<head>
<title>Preview - Todos los Días</title>
<style>
body{font-family:Arial;background:#f5f7fb;padding:30px}
.container{max-width:700px;margin:auto}
.card{background:#fff;padding:30px;border-radius:14px;box-shadow:0 8px 20px rgba(0,0,0,.08)}
table{width:100%;border-collapse:collapse;margin:20px 0}
th{text-align:left;padding:10px;background:#f5f7fb}
button{padding:18px 40px;font-size:18px;font-weight:700;border-radius:10px;border:none;cursor:pointer;background:#6610f2;color:#fff;width:100%;margin-top:10px}
button:hover{background:#5a0dd1}
a{display:inline-block;margin-top:15px;color:#666;text-decoration:none}
.total{font-size:20px;font-weight:700;color:#6610f2;margin:10px 0}
</style>
</head>
<body>
<div class="container">
<div class="card">
<h1>🚀 Preview — Enviar Todos los Días</h1>
<p>Se enviará a cada usuario el mensaje correspondiente a su día actual:</p>

<table>
  <thead>
    <tr>
      <th>Día</th>
      <th>Título de la notificación</th>
      <th>Usuarios</th>
    </tr>
  </thead>
  <tbody>${rows}</tbody>
</table>

<p class="total">Total a enviar: ${total} notificaciones</p>
<p style="color:#888;font-size:14px">Body para todos: "Toca para ver el estado de tu solicitud"</p>

<form method="POST" action="/send-all-days">
  <button onclick="return confirm('¿Confirmar envío a ${total} usuarios?')">
    ✅ CONFIRMAR ENVÍO A TODOS
  </button>
</form>

<a href="/">← Cancelar, volver al inicio</a>
</div>
</div>
</body>
</html>
  `);
});

/* =========================
   SEND ALL DAYS  ← NUEVO
========================= */

app.post("/send-all-days", async (req, res) => {
  try {
    const leadsSnap = await db.collection("leads").get();

    let totalSent = 0;
    let totalInvalidTokens = [];
    const results = [];

    for (const dayStr of Object.keys(DAY_PRESETS)) {
      const day = Number(dayStr);
      const title = DAY_PRESETS[day];
      const body = "Toca para ver el estado de tu solicitud";
      const screen = "/home";

      const tokens = leadsSnap.docs
        .filter((d) => calcDayFromCreatedAt(d.data().createdAt) === day)
        .map((d) => d.data().pushToken)
        .filter(Boolean);

      if (tokens.length === 0) {
        results.push(`Día ${day}: 0 usuarios`);
        continue;
      }

      const { sent, invalidTokens } = await sendPush(
        tokens,
        { title, body },
        screen
      );
      totalSent += sent;
      totalInvalidTokens = totalInvalidTokens.concat(invalidTokens);
      results.push(`Día ${day}: ${sent} enviadas`);
    }

    // Limpiar todos los tokens muertos encontrados en el proceso
    if (totalInvalidTokens.length > 0) {
      await cleanInvalidTokens(totalInvalidTokens);
      console.log(`🗑️ Limpiados ${totalInvalidTokens.length} tokens inválidos`);
    }

    const summary = results.join(" | ");
    const cleanMsg =
      totalInvalidTokens.length > 0
        ? ` — ${totalInvalidTokens.length} tokens limpiados`
        : "";

    res.redirect(
      `/?msg=${encodeURIComponent(
        `✅ Todos los días enviados: ${totalSent} notificaciones. ${summary}${cleanMsg}`
      )}`
    );
  } catch (e) {
    res.redirect(`/?error=${encodeURIComponent(e.message)}`);
  }
});

/* =========================
   PREVIEW
========================= */

app.post("/preview", (req, res) => {
  const { type, preset, day, manualTitle, manualBody } = req.body;

  let title, body;

  if (type === "no-lead") {
    const p = NO_LEAD_PRESETS[preset];
    title = p.title;
    body = p.body;
  } else if (type === "day") {
    title = DAY_PRESETS[day];
    body = "Toca para ver el estado de tu solicitud";
  } else {
    title = manualTitle;
    body = manualBody;
  }

  res.send(`
<h2>Preview</h2>
<h3>${title}</h3>
<p>${body}</p>

<form method="POST" action="/send">
<input type="hidden" name="type" value="${type}"/>
<input type="hidden" name="preset" value="${preset || ""}"/>
<input type="hidden" name="day" value="${day || ""}"/>
<input type="hidden" name="manualTitle" value="${manualTitle || ""}"/>
<input type="hidden" name="manualBody" value="${manualBody || ""}"/>
<button style="padding:16px;font-size:18px">CONFIRMAR ENVÍO</button>
</form>

<a href="/">Cancelar</a>
`);
});

/* =========================
   SEND
========================= */

app.post("/send", async (req, res) => {
  try {
    const { type, preset, day, manualTitle, manualBody } = req.body;

    let title,
      body,
      tokens = [];
    let screen = "/home";

    if (type === "no-lead") {
      const p = NO_LEAD_PRESETS[preset];
      title = p.title;
      body = p.body;
      screen = "/";

      const snap = await db.collection("devices").get();
      tokens = snap.docs
        .map((d) => d.data())
        .filter((d) => d.hasLead !== true)
        .map((d) => d.expo_push_token)
        .filter(Boolean);
    } else if (type === "day") {
      title = DAY_PRESETS[day];
      body = "Toca para ver el estado de tu solicitud";
      screen = "/home";

      const leads = await db.collection("leads").get();
      tokens = leads.docs
        .filter(
          (d) => calcDayFromCreatedAt(d.data().createdAt) === Number(day)
        )
        .map((d) => d.data().pushToken)
        .filter(Boolean);
    } else {
      title = manualTitle;
      body = manualBody;

      if (type === "lead") {
        const leads = await db.collection("leads").get();
        tokens = leads.docs.map((d) => d.data().pushToken).filter(Boolean);
      } else if (type === "no-lead") {
        screen = "/";
        const snap = await db.collection("devices").get();
        tokens = snap.docs
          .map((d) => d.data())
          .filter((d) => d.hasLead !== true)
          .map((d) => d.expo_push_token)
          .filter(Boolean);
      } else {
        const snap = await db.collection("devices").get();
        tokens = snap.docs
          .map((d) => d.data().expo_push_token)
          .filter(Boolean);
      }
    }

    const { sent, invalidTokens } = await sendPush(
      tokens,
      { title, body },
      screen
    );

    if (invalidTokens.length > 0) {
      await cleanInvalidTokens(invalidTokens);
      console.log(`🗑️ Limpiados ${invalidTokens.length} tokens inválidos`);
    }

    const message =
      invalidTokens.length > 0
        ? `Enviadas ${sent} notificaciones (${invalidTokens.length} tokens limpiados)`
        : `Enviadas ${sent} notificaciones`;

    res.redirect(`/?msg=${encodeURIComponent(message)}`);
  } catch (e) {
    res.redirect(`/?error=${encodeURIComponent(e.message)}`);
  }
});

/* =========================
   CLEAN TOKENS
========================= */

app.get("/clean-tokens", async (req, res) => {
  try {
    const allTokens = [];

    const devicesSnap = await db.collection("devices").get();
    devicesSnap.docs.forEach((d) => {
      const token = d.data().expo_push_token;
      if (token) allTokens.push(token);
    });

    const leadsSnap = await db.collection("leads").get();
    leadsSnap.docs.forEach((d) => {
      const token = d.data().pushToken;
      if (token) allTokens.push(token);
    });

    if (allTokens.length === 0) {
      return res.redirect(`/?msg=No hay tokens para validar`);
    }

    const invalidTokens = [];

    for (let i = 0; i < allTokens.length; i += 100) {
      const chunk = allTokens.slice(i, i + 100);

      const resp = await fetch("https://exp.host/--/api/v2/push/send", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(
          chunk.map((to) => ({
            to,
            channelId: "default",
            priority: "default",
            data: { silent: true },
          }))
        ),
      });

      const json = await resp.json();

      json.data.forEach((result, idx) => {
        if (result.status === "error") {
          const errorType = result.details?.error;
          if (
            errorType === "DeviceNotRegistered" ||
            errorType === "InvalidCredentials"
          ) {
            invalidTokens.push(chunk[idx]);
          }
        }
      });
    }

    const cleaned = await cleanInvalidTokens(invalidTokens);

    res.redirect(
      `/?msg=Limpieza completada: ${cleaned} tokens eliminados de ${allTokens.length} revisados`
    );
  } catch (e) {
    res.redirect(`/?error=${encodeURIComponent(e.message)}`);
  }
});

/* =========================
   DIAGNOSTICO
========================= */

app.get("/diagnostico", async (req, res) => {
  try {
    const devicesSnap = await db.collection("devices").get();
    const leadsSnap = await db.collection("leads").get();

    const devicesData = [];
    const devicesTokens = new Set();
    const devicesDuplicates = [];

    devicesSnap.docs.forEach((d) => {
      const data = d.data();
      const token = data.expo_push_token;

      if (token) {
        if (devicesTokens.has(token)) {
          devicesDuplicates.push({ id: d.id, token, hasLead: data.hasLead });
        }
        devicesTokens.add(token);
        devicesData.push({ id: d.id, token, hasLead: data.hasLead });
      }
    });

    const leadsData = [];
    const leadsTokens = new Set();
    const leadsDuplicates = [];
    let leadsWithoutToken = 0;
    let leadsWithoutCreatedAt = 0;

    leadsSnap.docs.forEach((d) => {
      const data = d.data();
      const token = data.pushToken;

      if (!token) {
        leadsWithoutToken++;
      } else {
        if (leadsTokens.has(token)) {
          leadsDuplicates.push({
            id: d.id,
            token,
            createdAt: data.createdAt,
          });
        }
        leadsTokens.add(token);
        leadsData.push({
          id: d.id,
          token,
          createdAt: data.createdAt,
          day: calcDayFromCreatedAt(data.createdAt),
        });
      }

      if (!data.createdAt) {
        leadsWithoutCreatedAt++;
      }
    });

    const tokensInBoth = [];
    devicesTokens.forEach((token) => {
      if (leadsTokens.has(token)) {
        tokensInBoth.push(token);
      }
    });

    const tokensOnlyInLeads = [];
    leadsTokens.forEach((token) => {
      if (!devicesTokens.has(token)) {
        tokensOnlyInLeads.push(token);
      }
    });

    const allTokens = new Set([...devicesTokens, ...leadsTokens]);

    res.send(`
<!DOCTYPE html>
<html>
<head>
<title>Diagnóstico de Tokens</title>
<style>
body{font-family:Arial;background:#f5f7fb;padding:30px}
.container{max-width:1000px;margin:auto}
.card{background:#fff;padding:20px;border-radius:14px;box-shadow:0 8px 20px rgba(0,0,0,.08);margin-bottom:20px}
h1{color:#333}
h2{color:#1FA971;border-bottom:2px solid #1FA971;padding-bottom:10px}
.stat{display:grid;grid-template-columns:2fr 1fr;padding:10px;border-bottom:1px solid #eee}
.stat:last-child{border:none}
.label{font-weight:600}
.value{text-align:right;color:#1FA971;font-weight:700}
.warning{color:#F6A800}
.danger{color:#dc3545}
.btn{display:inline-block;padding:12px 24px;background:#dc3545;color:#fff;text-decoration:none;border-radius:8px;font-weight:700;margin-top:10px}
.btn:hover{background:#c82333}
pre{background:#f8f9fa;padding:15px;border-radius:8px;overflow-x:auto;font-size:12px}
</style>
</head>
<body>
<div class="container">
<h1>🔍 Diagnóstico Completo de Tokens</h1>

<div class="card">
<h2>📊 Resumen General</h2>
<div class="stat"><span class="label">Documentos en "devices":</span><span class="value">${devicesSnap.size}</span></div>
<div class="stat"><span class="label">Tokens en "devices":</span><span class="value">${devicesTokens.size}</span></div>
<div class="stat"><span class="label">Documentos en "leads":</span><span class="value">${leadsSnap.size}</span></div>
<div class="stat"><span class="label">Tokens en "leads":</span><span class="value">${leadsTokens.size}</span></div>
<div class="stat"><span class="label">Leads sin token:</span><span class="value warning">${leadsWithoutToken}</span></div>
<div class="stat"><span class="label">Leads sin createdAt:</span><span class="value warning">${leadsWithoutCreatedAt}</span></div>
<div class="stat"><span class="label">✅ Tokens únicos totales:</span><span class="value">${allTokens.size}</span></div>
</div>

<div class="card">
<h2>⚠️ Problemas Detectados</h2>
<div class="stat"><span class="label">Duplicados en "devices":</span><span class="value danger">${devicesDuplicates.length}</span></div>
<div class="stat"><span class="label">Duplicados en "leads":</span><span class="value danger">${leadsDuplicates.length}</span></div>
<div class="stat"><span class="label">Tokens en ambas colecciones:</span><span class="value warning">${tokensInBoth.length}</span></div>
<div class="stat"><span class="label">Tokens SOLO en leads (raro):</span><span class="value danger">${tokensOnlyInLeads.length}</span></div>
</div>

${
  devicesDuplicates.length > 0
    ? `
<div class="card">
<h2>🔴 Duplicados en Devices</h2>
<pre>${JSON.stringify(devicesDuplicates, null, 2)}</pre>
</div>
`
    : ""
}

${
  leadsDuplicates.length > 0
    ? `
<div class="card">
<h2>🔴 Duplicados en Leads</h2>
<pre>${JSON.stringify(leadsDuplicates, null, 2)}</pre>
</div>
`
    : ""
}

${
  tokensOnlyInLeads.length > 0
    ? `
<div class="card">
<h2>🔴 Tokens SOLO en Leads (sin device)</h2>
<p>Estos tokens están en leads pero NO en devices. Esto es extraño y puede causar problemas.</p>
<pre>${JSON.stringify(tokensOnlyInLeads.slice(0, 10), null, 2)}</pre>
${tokensOnlyInLeads.length > 10 ? `<p>... y ${tokensOnlyInLeads.length - 10} más</p>` : ""}
</div>
`
    : ""
}

<div class="card">
<h2>🧹 Acciones Disponibles</h2>
<p>Si encuentras duplicados o inconsistencias, puedes limpiarlos:</p>
<a href="/fix-duplicates" class="btn" onclick="return confirm('¿Eliminar duplicados y normalizar la base de datos?')">
🔧 Arreglar Duplicados
</a>
</div>

<a href="/" style="display:inline-block;margin-top:20px">← Volver al Control Center</a>
</div>
</body>
</html>
    `);
  } catch (e) {
    res.send(`<h1>Error</h1><pre>${e.message}</pre><a href="/">Volver</a>`);
  }
});

/* =========================
   FIX DUPLICATES
========================= */

app.get("/fix-duplicates", async (req, res) => {
  try {
    const devicesSnap = await db.collection("devices").get();
    const leadsSnap = await db.collection("leads").get();

    const batch = db.batch();
    let fixed = 0;

    const devicesTokenMap = new Map();
    const leadsTokenMap = new Map();

    devicesSnap.docs.forEach((doc) => {
      const token = doc.data().expo_push_token;
      if (token) {
        if (!devicesTokenMap.has(token)) {
          devicesTokenMap.set(token, []);
        }
        devicesTokenMap.get(token).push(doc);
      }
    });

    leadsSnap.docs.forEach((doc) => {
      const token = doc.data().pushToken;
      if (token) {
        if (!leadsTokenMap.has(token)) {
          leadsTokenMap.set(token, []);
        }
        leadsTokenMap.get(token).push(doc);
      }
    });

    devicesTokenMap.forEach((docs, token) => {
      if (docs.length > 1) {
        for (let i = 1; i < docs.length; i++) {
          batch.delete(docs[i].ref);
          fixed++;
        }
      }
    });

    leadsTokenMap.forEach((docs, token) => {
      if (docs.length > 1) {
        docs.sort((a, b) => {
          const dateA = a.data().createdAt?.toDate() || new Date(0);
          const dateB = b.data().createdAt?.toDate() || new Date(0);
          return dateB - dateA;
        });

        for (let i = 1; i < docs.length; i++) {
          batch.delete(docs[i].ref);
          fixed++;
        }
      }
    });

    await batch.commit();

    res.redirect(
      `/?msg=✅ Base de datos normalizada: ${fixed} duplicados eliminados`
    );
  } catch (e) {
    res.redirect(`/?error=${encodeURIComponent(e.message)}`);
  }
});

/* =========================
   START
========================= */

app.listen(3000, () =>
  console.log("🔥 Push Control Center corriendo en http://localhost:3000")
);
