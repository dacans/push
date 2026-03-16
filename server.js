const express = require("express");
const cors = require("cors");
const admin = require("firebase-admin");
const fetch = require("node-fetch");

admin.initializeApp({
  credential: admin.credential.cert(
    process.env.SERVICE_ACCOUNT_KEY
      ? JSON.parse(process.env.SERVICE_ACCOUNT_KEY)
      : require("./serviceAccountKey.json")
  ),
});

const db = admin.firestore();
const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

/* =========================
   CONSTANTS
========================= */

const COUNTRIES = ["MX", "ES", "CO"];
const LEAD_MAX_DAYS = 15; // leads older than this get purged

const NO_LEAD_PRESETS = {
  finish:    { title: "⚠️ Completa tu solicitud", body: "Iniciaste una solicitud pero no la terminaste. Complétala ahora." },
  dont_miss: { title: "⏳ No la dejes pasar",     body: "Tu solicitud aún no está completa. Toma solo 2 minutos." },
  almost:    { title: "🚀 Casi listo",             body: "Estás a un paso de completar tu solicitud." },
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

function ageInDays(createdAt) {
  if (!createdAt) return null;
  const start = createdAt.toDate ? createdAt.toDate() : new Date(createdAt);
  return Math.floor((Date.now() - start.getTime()) / 86400000);
}

/* Send push to array of tokens, returns { sent, invalidTokens } */
async function sendPush(tokens, payload, screen) {
  let sent = 0;
  const invalidTokens = [];

  for (let i = 0; i < tokens.length; i += 100) {
    const chunk = tokens.slice(i, i + 100);
    const resp = await fetch("https://exp.host/--/api/v2/push/send", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(chunk.map((to) => ({
        to, sound: "default",
        title: payload.title, body: payload.body, data: { screen },
      }))),
    });

    const json = await resp.json();
    if (!resp.ok) throw new Error(JSON.stringify(json));

    json.data.forEach((result, idx) => {
      if (result.status === "error") {
        const e = result.details?.error;
        if (e === "DeviceNotRegistered" || e === "InvalidCredentials") {
          invalidTokens.push(chunk[idx]);
        }
      } else if (result.status === "ok") {
        sent++;
      }
    });
  }
  return { sent, invalidTokens };
}

/* Remove invalid tokens using targeted queries instead of full scan */
async function cleanInvalidTokens(invalidTokens) {
  if (!invalidTokens.length) return 0;
  const batch = db.batch();
  let cleaned = 0;

  for (const token of invalidTokens) {
    // Query devices by token field — no full scan
    const devSnap = await db.collection("devices")
      .where("expo_push_token", "==", token).get();
    devSnap.docs.forEach((d) => { batch.delete(d.ref); cleaned++; });

    // Query leads by pushToken field
    const leadSnap = await db.collection("leads")
      .where("pushToken", "==", token).get();
    leadSnap.docs.forEach((d) => {
      batch.update(d.ref, { pushToken: admin.firestore.FieldValue.delete() });
      cleaned++;
    });
  }

  await batch.commit();
  return cleaned;
}

/* Load all devices and leads once, return structured stats */
async function loadStats() {
  const [devicesSnap, leadsSnap] = await Promise.all([
    db.collection("devices").get(),
    db.collection("leads").get(),
  ]);

  // Per-country stats
  const byCountry = {};
  COUNTRIES.forEach((c) => {
    byCountry[c] = { devices: 0, noLead: 0, leads: 0, days: { 1:0,2:0,3:0,4:0,5:0,6:0,7:0 } };
  });
  byCountry["?"] = { devices: 0, noLead: 0, leads: 0, days: { 1:0,2:0,3:0,4:0,5:0,6:0,7:0 } };

  devicesSnap.docs.forEach((d) => {
    const data = d.data();
    const c = COUNTRIES.includes(data.country) ? data.country : "?";
    byCountry[c].devices++;
    if (data.hasLead !== true) byCountry[c].noLead++;
  });

  leadsSnap.docs.forEach((d) => {
    const data = d.data();
    const c = COUNTRIES.includes(data.country) ? data.country : "?";
    byCountry[c].leads++;
    const day = calcDayFromCreatedAt(data.createdAt);
    if (day) byCountry[c].days[day]++;
  });

  const totalDevices = devicesSnap.size;
  const totalLeads   = leadsSnap.size;
  const totalNoLead  = devicesSnap.docs.filter((d) => d.data().hasLead !== true).length;

  return { byCountry, totalDevices, totalLeads, totalNoLead, devicesSnap, leadsSnap };
}

/* =========================
   CONTROL CENTER (UI)
========================= */

app.get("/", async (req, res) => {
  const { msg, error } = req.query;

  const { byCountry, totalDevices, totalLeads, totalNoLead } = await loadStats();

  const countryFlag = { MX: "🇲🇽", ES: "🇪🇸", CO: "🇨🇴", "?": "❓" };

  const countryStatsHtml = [...COUNTRIES, "?"].map((c) => {
    const s = byCountry[c];
    if (!s.devices && !s.leads) return "";
    return `
    <div class="card">
      <h2>${countryFlag[c]} ${c === "?" ? "Sin país" : c}</h2>
      <p class="stat">📱 Devices: ${s.devices}</p>
      <p class="stat">🚫 Sin solicitud: ${s.noLead}</p>
      <p class="stat">📋 Leads: ${s.leads}</p>
      <hr style="border:none;border-top:1px solid #eee;margin:10px 0"/>
      ${Object.entries(s.days).map(([d,v]) => `<p class="stat" style="font-size:13px">Día ${d}: ${v}</p>`).join("")}
    </div>`;
  }).join("");

  res.send(`
<!DOCTYPE html>
<html>
<head>
<title>Push Control Center</title>
<style>
body{font-family:Arial;background:#f5f7fb;padding:30px}
.container{max-width:1400px;margin:auto}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:20px;margin-bottom:30px}
.card{background:#fff;padding:20px;border-radius:14px;box-shadow:0 8px 20px rgba(0,0,0,.08)}
h1{margin-bottom:8px} h2{margin-top:0}
button{width:100%;padding:14px;font-size:15px;font-weight:700;border-radius:10px;border:none;margin-top:8px;cursor:pointer}
.primary{background:#1FA971;color:#fff} .warn{background:#F6A800;color:#fff}
.danger{background:#dc3545;color:#fff} .purple{background:#6610f2;color:#fff;font-size:16px;padding:16px;margin-top:12px}
input,textarea,select{width:100%;padding:12px;font-size:15px;border-radius:8px;border:1px solid #ccc;margin-top:6px}
textarea{min-height:80px}
.ok{background:#e7f9ed;color:#1e7e34;padding:12px;border-radius:8px;margin-bottom:20px}
.err{background:#fdecea;color:#b21f2d;padding:12px;border-radius:8px;margin-bottom:20px}
.stat{font-weight:600;margin:4px 0;font-size:14px}
.divider{border:none;border-top:2px solid #eee;margin:14px 0}
.section-title{font-size:18px;font-weight:800;margin:28px 0 12px;color:#333;border-left:4px solid #1FA971;padding-left:10px}
.totals{display:flex;gap:16px;flex-wrap:wrap;margin-bottom:24px}
.total-pill{background:#fff;border-radius:12px;padding:14px 22px;box-shadow:0 4px 12px rgba(0,0,0,.07);font-weight:700;font-size:15px}
.total-pill span{color:#1FA971;font-size:22px;display:block}
</style>
</head>
<body>
<div class="container">
<h1>📣 Push Control Center</h1>

${msg   ? `<div class="ok">${msg}</div>`   : ""}
${error ? `<div class="err">${error}</div>` : ""}

<!-- TOTALS -->
<div class="totals">
  <div class="total-pill"><span>${totalDevices}</span>Total Devices</div>
  <div class="total-pill"><span>${totalLeads}</span>Total Leads</div>
  <div class="total-pill"><span>${totalNoLead}</span>Sin solicitud</div>
</div>

<!-- STATS POR PAÍS -->
<div class="section-title">📊 Stats por País</div>
<div class="grid">${countryStatsHtml}</div>

<!-- PUSH ACTIONS -->
<div class="section-title">📤 Enviar Notificaciones</div>
<div class="grid">

  <!-- Sin solicitud -->
  <div class="card">
    <h2>Sin solicitud</h2>
    <form method="POST" action="/preview">
      <input type="hidden" name="type" value="no-lead"/>
      <label>País</label>
      <select name="country">
        <option value="ALL">🌎 Todos</option>
        <option value="MX">🇲🇽 México</option>
        <option value="ES">🇪🇸 España</option>
        <option value="CO">🇨🇴 Colombia</option>
      </select>
      <button name="preset" value="finish" class="warn">⚠️ Completar solicitud</button>
      <button name="preset" value="dont_miss" class="warn">⏳ No la dejes pasar</button>
      <button name="preset" value="almost" class="warn">🚀 Casi listo</button>
    </form>
  </div>

  <!-- Por día -->
  <div class="card">
    <h2>Solicitudes día 1–7</h2>
    <form method="POST" action="/preview">
      <input type="hidden" name="type" value="day"/>
      <label>País</label>
      <select name="country">
        <option value="ALL">🌎 Todos</option>
        <option value="MX">🇲🇽 México</option>
        <option value="ES">🇪🇸 España</option>
        <option value="CO">🇨🇴 Colombia</option>
      </select>
      ${Object.keys(DAY_PRESETS).map((d) =>
        `<button name="day" value="${d}" class="primary">Día ${d}</button>`
      ).join("")}
    </form>
    <hr class="divider"/>
    <form method="POST" action="/preview-all-days">
      <select name="country">
        <option value="ALL">🌎 Todos</option>
        <option value="MX">🇲🇽 México</option>
        <option value="ES">🇪🇸 España</option>
        <option value="CO">🇨🇴 Colombia</option>
      </select>
      <button class="purple" onclick="return confirm('¿Enviar a TODOS los leads (días 1–7)?')">
        🚀 Enviar Todos los Días (1–7)
      </button>
    </form>
  </div>

  <!-- Manual -->
  <div class="card">
    <h2>Envío manual</h2>
    <form method="POST" action="/preview">
      <label>Título</label><input name="manualTitle" required/>
      <label>Mensaje</label><textarea name="manualBody" required></textarea>
      <label>Audiencia</label>
      <select name="type">
        <option value="all">Todos</option>
        <option value="no-lead">Sin solicitud</option>
        <option value="lead">Con solicitud</option>
      </select>
      <label>País</label>
      <select name="country">
        <option value="ALL">🌎 Todos</option>
        <option value="MX">🇲🇽 México</option>
        <option value="ES">🇪🇸 España</option>
        <option value="CO">🇨🇴 Colombia</option>
      </select>
      <button class="primary">Preview</button>
    </form>
  </div>

  <!-- Mantenimiento -->
  <div class="card">
    <h2>🗑️ Mantenimiento DB</h2>
    <p style="font-size:13px;color:#666;margin-bottom:8px">Limpia tokens inválidos (DeviceNotRegistered)</p>
    <form method="GET" action="/clean-tokens">
      <button class="danger" onclick="return confirm('¿Limpiar tokens muertos?')">🧹 Limpiar tokens inválidos</button>
    </form>
    <hr class="divider"/>
    <p style="font-size:13px;color:#666;margin-bottom:8px">Elimina leads sin pushToken</p>
    <form method="GET" action="/purge-null-tokens" onsubmit="return confirm('¿Eliminar todos los leads sin pushToken?')">
      <button class="danger">🗑️ Purgar leads sin token</button>
    </form>
    <hr class="divider"/>
    <p style="font-size:13px;color:#666;margin-bottom:8px">Elimina leads y devices con +${LEAD_MAX_DAYS} días</p>
    <form method="GET" action="/purge-old" onsubmit="return confirm('¿Eliminar registros con más de ${LEAD_MAX_DAYS} días?')">
      <button class="danger">🗑️ Purgar registros viejos (+${LEAD_MAX_DAYS} días)</button>
    </form>
    <hr class="divider"/>
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
   PREVIEW (with country filter)
========================= */

app.post("/preview", async (req, res) => {
  try {
    const { type, preset, day, manualTitle, manualBody, country } = req.body;
    const filterCountry = country && country !== "ALL" ? country : null;

    let payload, tokens = [], targetLabel;

    if (type === "no-lead") {
      payload = NO_LEAD_PRESETS[preset];
      if (!payload) return res.redirect("/?error=Preset inválido");

      let query = db.collection("devices").where("hasLead", "!=", true);
      const snap = await query.get();
      snap.docs.forEach((d) => {
        const data = d.data();
        if (filterCountry && data.country !== filterCountry) return;
        if (data.expo_push_token) tokens.push(data.expo_push_token);
      });
      targetLabel = `sin solicitud${filterCountry ? ` (${filterCountry})` : ""}`;

    } else if (type === "day") {
      const dayNum = parseInt(day);
      if (!dayNum) return res.redirect("/?error=Día inválido");
      payload = { title: DAY_PRESETS[dayNum], body: `Actualización del día ${dayNum} de tu solicitud.` };

      const leadsSnap = await db.collection("leads").get();
      leadsSnap.docs.forEach((d) => {
        const data = d.data();
        if (filterCountry && data.country !== filterCountry) return;
        if (calcDayFromCreatedAt(data.createdAt) === dayNum && data.pushToken) {
          tokens.push(data.pushToken);
        }
      });
      targetLabel = `día ${dayNum}${filterCountry ? ` (${filterCountry})` : ""}`;

    } else if (type === "lead" || type === "all") {
      payload = { title: manualTitle, body: manualBody };
      if (type === "all" || type === "lead") {
        const leadsSnap = await db.collection("leads").get();
        leadsSnap.docs.forEach((d) => {
          const data = d.data();
          if (filterCountry && data.country !== filterCountry) return;
          if (data.pushToken) tokens.push(data.pushToken);
        });
      }
      if (type === "all" || type === "no-lead") {
        const devSnap = await db.collection("devices").get();
        devSnap.docs.forEach((d) => {
          const data = d.data();
          if (filterCountry && data.country !== filterCountry) return;
          if (data.hasLead !== true && data.expo_push_token) tokens.push(data.expo_push_token);
        });
      }
      targetLabel = `manual${filterCountry ? ` (${filterCountry})` : ""}`;
    } else {
      payload = { title: manualTitle, body: manualBody };
      targetLabel = "manual";
    }

    // Deduplicate tokens
    tokens = [...new Set(tokens)];

    res.send(`
<!DOCTYPE html><html><head><title>Preview</title>
<style>body{font-family:Arial;background:#f5f7fb;padding:30px}.container{max-width:700px;margin:auto}
.card{background:#fff;padding:24px;border-radius:14px;box-shadow:0 8px 20px rgba(0,0,0,.08)}
button{padding:14px 28px;font-size:16px;font-weight:700;border-radius:10px;border:none;cursor:pointer;margin-right:10px}
.primary{background:#1FA971;color:#fff}.secondary{background:#eee;color:#333}</style></head>
<body><div class="container">
<h1>👀 Preview: ${targetLabel}</h1>
<div class="card">
  <p><strong>Título:</strong> ${payload.title}</p>
  <p><strong>Mensaje:</strong> ${payload.body}</p>
  <p><strong>Destinatarios:</strong> ${tokens.length} tokens</p>
  <form method="POST" action="/send">
    <input type="hidden" name="title" value="${payload.title}"/>
    <input type="hidden" name="body" value="${payload.body}"/>
    <input type="hidden" name="tokens" value="${tokens.join(",")}"/>
    <input type="hidden" name="screen" value="application-status"/>
    <button class="primary" onclick="return confirm('¿Enviar a ${tokens.length} usuarios?')">
      ✅ Confirmar envío
    </button>
    <a href="/"><button type="button" class="secondary">Cancelar</button></a>
  </form>
</div></div></body></html>`);
  } catch (e) {
    res.redirect(`/?error=${encodeURIComponent(e.message)}`);
  }
});

/* =========================
   SEND
========================= */

app.post("/send", async (req, res) => {
  try {
    const { title, body, tokens: rawTokens, screen } = req.body;
    const tokens = rawTokens ? rawTokens.split(",").filter(Boolean) : [];
    if (!tokens.length) return res.redirect("/?error=Sin tokens");

    const { sent, invalidTokens } = await sendPush({ title, body }, { title, body }, screen || "home");
    const cleaned = await cleanInvalidTokens(invalidTokens);

    res.redirect(`/?msg=✅ Enviado a ${sent} usuarios. ${cleaned} tokens inválidos limpiados.`);
  } catch (e) {
    res.redirect(`/?error=${encodeURIComponent(e.message)}`);
  }
});

/* =========================
   PREVIEW ALL DAYS (with country)
========================= */

app.post("/preview-all-days", async (req, res) => {
  try {
    const filterCountry = req.body.country && req.body.country !== "ALL" ? req.body.country : null;
    const leadsSnap = await db.collection("leads").get();

    const dayTokens = { 1:[], 2:[], 3:[], 4:[], 5:[], 6:[], 7:[] };

    leadsSnap.docs.forEach((d) => {
      const data = d.data();
      if (filterCountry && data.country !== filterCountry) return;
      const day = calcDayFromCreatedAt(data.createdAt);
      if (day && data.pushToken) dayTokens[day].push(data.pushToken);
    });

    const total = Object.values(dayTokens).flat().length;
    const countryLabel = filterCountry || "Todos";

    res.send(`
<!DOCTYPE html><html><head><title>Preview Todos los Días</title>
<style>body{font-family:Arial;background:#f5f7fb;padding:30px}.container{max-width:700px;margin:auto}
.card{background:#fff;padding:24px;border-radius:14px;box-shadow:0 8px 20px rgba(0,0,0,.08);margin-bottom:16px}
button{padding:14px 28px;font-size:16px;font-weight:700;border-radius:10px;border:none;cursor:pointer;margin-right:10px}
.primary{background:#1FA971;color:#fff}.secondary{background:#eee;color:#333}</style></head>
<body><div class="container">
<h1>🚀 Preview — Todos los días (${countryLabel})</h1>
<div class="card">
  ${Object.entries(dayTokens).map(([d,t]) =>
    `<p><strong>Día ${d}:</strong> ${t.length} usuarios — "${DAY_PRESETS[d]}"</p>`
  ).join("")}
  <p><strong>Total: ${total} envíos</strong></p>
  <form method="POST" action="/send-all-days">
    <input type="hidden" name="country" value="${filterCountry || 'ALL'}"/>
    <button class="primary" onclick="return confirm('¿Enviar a ${total} usuarios en total?')">
      ✅ Confirmar envío masivo
    </button>
    <a href="/"><button type="button" class="secondary">Cancelar</button></a>
  </form>
</div></div></body></html>`);
  } catch (e) {
    res.redirect(`/?error=${encodeURIComponent(e.message)}`);
  }
});

/* =========================
   SEND ALL DAYS
========================= */

app.post("/send-all-days", async (req, res) => {
  try {
    const filterCountry = req.body.country && req.body.country !== "ALL" ? req.body.country : null;
    const leadsSnap = await db.collection("leads").get();

    const dayTokens = { 1:[], 2:[], 3:[], 4:[], 5:[], 6:[], 7:[] };
    leadsSnap.docs.forEach((d) => {
      const data = d.data();
      if (filterCountry && data.country !== filterCountry) return;
      const day = calcDayFromCreatedAt(data.createdAt);
      if (day && data.pushToken) dayTokens[day].push(data.pushToken);
    });

    let totalSent = 0;
    let totalInvalid = [];

    for (const [day, tokens] of Object.entries(dayTokens)) {
      if (!tokens.length) continue;
      const payload = { title: DAY_PRESETS[day], body: `Actualización del día ${day} de tu solicitud.` };
      const { sent, invalidTokens } = await sendPush(tokens, payload, "application-status");
      totalSent += sent;
      totalInvalid = totalInvalid.concat(invalidTokens);
    }

    const cleaned = await cleanInvalidTokens(totalInvalid);
    res.redirect(`/?msg=✅ Enviado a ${totalSent} usuarios (todos los días). ${cleaned} tokens inválidos limpiados.`);
  } catch (e) {
    res.redirect(`/?error=${encodeURIComponent(e.message)}`);
  }
});

/* =========================
   PURGE: LEADS SIN PUSH TOKEN
========================= */

app.get("/purge-null-tokens", async (req, res) => {
  try {
    const leadsSnap = await db.collection("leads")
      .where("pushToken", "==", null).get();

    const batch = db.batch();
    let count = 0;

    leadsSnap.docs.forEach((d) => {
      batch.delete(d.ref);
      count++;
    });

    // Also catch leads where pushToken field doesn't exist
    const leadsSnap2 = await db.collection("leads").get();
    leadsSnap2.docs.forEach((d) => {
      const data = d.data();
      if (!data.pushToken && !leadsSnap.docs.find(x => x.id === d.id)) {
        batch.delete(d.ref);
        count++;
      }
    });

    await batch.commit();
    res.redirect(`/?msg=🗑️ ${count} leads sin pushToken eliminados`);
  } catch (e) {
    res.redirect(`/?error=${encodeURIComponent(e.message)}`);
  }
});

/* =========================
   PURGE: REGISTROS VIEJOS (+15 DÍAS)
========================= */

app.get("/purge-old", async (req, res) => {
  try {
    const cutoff = new Date(Date.now() - LEAD_MAX_DAYS * 86400000);
    const cutoffTimestamp = admin.firestore.Timestamp.fromDate(cutoff);

    const batch = db.batch();
    let leadsDeleted = 0;
    let devicesDeleted = 0;

    // Leads con createdAt > 15 días
    const oldLeads = await db.collection("leads")
      .where("createdAt", "<", cutoffTimestamp).get();
    oldLeads.docs.forEach((d) => {
      batch.delete(d.ref);
      leadsDeleted++;
    });

    // Devices con updated_at > 15 días (inactivos)
    const oldDevices = await db.collection("devices")
      .where("updated_at", "<", cutoffTimestamp).get();
    oldDevices.docs.forEach((d) => {
      batch.delete(d.ref);
      devicesDeleted++;
    });

    await batch.commit();
    res.redirect(`/?msg=🗑️ Purga completada: ${leadsDeleted} leads y ${devicesDeleted} devices eliminados (más de ${LEAD_MAX_DAYS} días)`);
  } catch (e) {
    res.redirect(`/?error=${encodeURIComponent(e.message)}`);
  }
});

/* =========================
   CLEAN INVALID TOKENS (validate via Expo)
========================= */

app.get("/clean-tokens", async (req, res) => {
  try {
    const devicesSnap = await db.collection("devices").get();
    const leadsSnap = await db.collection("leads").get();

    const allTokens = new Set();
    devicesSnap.docs.forEach((d) => { if (d.data().expo_push_token) allTokens.add(d.data().expo_push_token); });
    leadsSnap.docs.forEach((d) => { if (d.data().pushToken) allTokens.add(d.data().pushToken); });

    const tokenArr = [...allTokens];
    const invalidTokens = [];

    for (let i = 0; i < tokenArr.length; i += 100) {
      const chunk = tokenArr.slice(i, i + 100);
      const resp = await fetch("https://exp.host/--/api/v2/push/send", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(chunk.map((to) => ({ to, title: "test", body: "test" }))),
      });
      const json = await resp.json();
      json.data.forEach((result, idx) => {
        const e = result.details?.error;
        if (e === "DeviceNotRegistered" || e === "InvalidCredentials") {
          invalidTokens.push(chunk[idx]);
        }
      });
    }

    const cleaned = await cleanInvalidTokens(invalidTokens);
    res.redirect(`/?msg=✅ Limpieza completada: ${cleaned} tokens inválidos eliminados de ${tokenArr.length} revisados`);
  } catch (e) {
    res.redirect(`/?error=${encodeURIComponent(e.message)}`);
  }
});

/* =========================
   DIAGNOSTICO
========================= */

app.get("/diagnostico", async (req, res) => {
  try {
    const { byCountry, totalDevices, totalLeads, devicesSnap, leadsSnap } = await loadStats();

    const devicesTokens = new Set();
    const devicesDuplicates = [];
    devicesSnap.docs.forEach((d) => {
      const token = d.data().expo_push_token;
      if (token) {
        if (devicesTokens.has(token)) devicesDuplicates.push({ id: d.id, token });
        devicesTokens.add(token);
      }
    });

    const leadsTokens = new Set();
    const leadsDuplicates = [];
    let leadsWithoutToken = 0;
    let leadsWithoutCreatedAt = 0;
    leadsSnap.docs.forEach((d) => {
      const data = d.data();
      if (!data.pushToken) { leadsWithoutToken++; return; }
      if (leadsTokens.has(data.pushToken)) leadsDuplicates.push({ id: d.id, token: data.pushToken });
      leadsTokens.add(data.pushToken);
      if (!data.createdAt) leadsWithoutCreatedAt++;
    });

    const countryFlag = { MX: "🇲🇽", ES: "🇪🇸", CO: "🇨🇴", "?": "❓" };

    res.send(`
<!DOCTYPE html><html><head><title>Diagnóstico</title>
<style>
body{font-family:Arial;background:#f5f7fb;padding:30px}
.container{max-width:1000px;margin:auto}
.card{background:#fff;padding:20px;border-radius:14px;box-shadow:0 8px 20px rgba(0,0,0,.08);margin-bottom:20px}
h1{color:#333} h2{color:#1FA971;border-bottom:2px solid #1FA971;padding-bottom:10px}
.stat{display:grid;grid-template-columns:2fr 1fr;padding:10px;border-bottom:1px solid #eee}
.stat:last-child{border:none} .label{font-weight:600} .value{text-align:right;color:#1FA971;font-weight:700}
.warning{color:#F6A800} .danger{color:#dc3545}
.btn{display:inline-block;padding:12px 20px;background:#dc3545;color:#fff;text-decoration:none;border-radius:8px;font-weight:700;margin:4px}
.btn.green{background:#1FA971} pre{background:#f8f9fa;padding:15px;border-radius:8px;overflow-x:auto;font-size:12px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:16px}
</style></head><body><div class="container">
<h1>🔍 Diagnóstico Completo</h1>

<div class="card">
<h2>📊 Resumen General</h2>
<div class="stat"><span class="label">Total Devices:</span><span class="value">${totalDevices}</span></div>
<div class="stat"><span class="label">Total Leads:</span><span class="value">${totalLeads}</span></div>
<div class="stat"><span class="label">Leads sin pushToken:</span><span class="value warning">${leadsWithoutToken}</span></div>
<div class="stat"><span class="label">Leads sin createdAt:</span><span class="value warning">${leadsWithoutCreatedAt}</span></div>
<div class="stat"><span class="label">Duplicados en devices:</span><span class="value danger">${devicesDuplicates.length}</span></div>
<div class="stat"><span class="label">Duplicados en leads:</span><span class="value danger">${leadsDuplicates.length}</span></div>
</div>

<div class="card">
<h2>🌎 Breakdown por País</h2>
<div class="grid">
  ${[...COUNTRIES, "?"].map((c) => {
    const s = byCountry[c];
    return `<div style="background:#f8f9fa;padding:14px;border-radius:10px">
      <strong>${countryFlag[c]} ${c}</strong>
      <p style="margin:6px 0;font-size:13px">📱 ${s.devices} devices</p>
      <p style="margin:6px 0;font-size:13px">📋 ${s.leads} leads</p>
      <p style="margin:6px 0;font-size:13px">🚫 ${s.noLead} sin solicitud</p>
    </div>`;
  }).join("")}
</div>
</div>

<div class="card">
<h2>🧹 Acciones</h2>
<a href="/fix-duplicates" class="btn" onclick="return confirm('¿Eliminar duplicados?')">🔧 Arreglar duplicados</a>
<a href="/purge-null-tokens" class="btn" onclick="return confirm('¿Purgar leads sin token?')">🗑️ Purgar sin token</a>
<a href="/purge-old" class="btn" onclick="return confirm('¿Purgar registros viejos?')">🗑️ Purgar +${LEAD_MAX_DAYS} días</a>
</div>

<a href="/">← Volver</a>
</div></body></html>`);
  } catch (e) {
    res.send(`<h1>Error</h1><pre>${e.message}</pre><a href="/">Volver</a>`);
  }
});

/* =========================
   FIX DUPLICATES
========================= */

app.get("/fix-duplicates", async (req, res) => {
  try {
    const [devicesSnap, leadsSnap] = await Promise.all([
      db.collection("devices").get(),
      db.collection("leads").get(),
    ]);

    const batch = db.batch();
    let fixed = 0;

    const devMap = new Map();
    devicesSnap.docs.forEach((d) => {
      const token = d.data().expo_push_token;
      if (!token) return;
      if (!devMap.has(token)) devMap.set(token, []);
      devMap.get(token).push(d);
    });
    devMap.forEach((docs) => {
      if (docs.length > 1) {
        for (let i = 1; i < docs.length; i++) { batch.delete(docs[i].ref); fixed++; }
      }
    });

    const leadMap = new Map();
    leadsSnap.docs.forEach((d) => {
      const token = d.data().pushToken;
      if (!token) return;
      if (!leadMap.has(token)) leadMap.set(token, []);
      leadMap.get(token).push(d);
    });
    leadMap.forEach((docs) => {
      if (docs.length > 1) {
        docs.sort((a, b) => {
          const da = a.data().createdAt?.toDate() || new Date(0);
          const db2 = b.data().createdAt?.toDate() || new Date(0);
          return db2 - da;
        });
        for (let i = 1; i < docs.length; i++) { batch.delete(docs[i].ref); fixed++; }
      }
    });

    await batch.commit();
    res.redirect(`/?msg=✅ ${fixed} duplicados eliminados`);
  } catch (e) {
    res.redirect(`/?error=${encodeURIComponent(e.message)}`);
  }
});

/* =========================
   START
========================= */

app.listen(3000, () =>
  console.log("🔥 Push Control Center en http://localhost:3000")
);