import { pool } from "./db.js";

function containsAny(text, words) {
  const t = text.toLowerCase();
  return words.some((w) => t.includes(w));
}

async function latestStats(corridorId) {
  const { rows } = await pool.query(
    `SELECT run_ts, fused_score, predicted_next_score_p50, predicted_next_score_p90, prediction_confidence,
            incidents_count, closures_count, weather_risk_score, weather_component, alert_state,
            raw_json
       FROM monitor_runs
      WHERE corridor_id = $1
      ORDER BY run_ts DESC
      LIMIT 1`,
    [corridorId]
  );
  return rows[0] || null;
}

async function trendStats(corridorId, hours = 24) {
  const { rows } = await pool.query(
    `SELECT AVG(fused_score) AS avg_score,
            MAX(fused_score) AS max_score,
            COUNT(*) AS points,
            AVG(predicted_next_score_p50) AS avg_p50,
            AVG(weather_risk_score) AS avg_weather_risk
       FROM monitor_runs
      WHERE corridor_id = $1
        AND run_ts >= NOW() - ($2::text || ' hours')::interval`,
    [corridorId, hours]
  );
  return rows[0] || null;
}

async function topCameras(corridorId, limit = 5) {
  const { rows } = await pool.query(
    `SELECT o.camera_location, AVG(o.image_score) AS avg_image_score
       FROM camera_observations o
       JOIN monitor_runs r ON r.id = o.run_id
      WHERE r.corridor_id = $1
        AND r.run_ts >= NOW() - INTERVAL '6 hours'
      GROUP BY o.camera_location
      ORDER BY AVG(o.image_score) DESC
      LIMIT $2`,
    [corridorId, limit]
  );
  return rows;
}

async function maybeAskOpenAI(question, context) {
  const key = process.env.OPENAI_API_KEY;
  if (!key) return { text: null, error: "OPENAI_API_KEY missing" };
  const model = process.env.OPENAI_MODEL || "gpt-5.2";
  const prompt = `Question: ${question}\nContext JSON:\n${JSON.stringify(context)}`;
  const instructions = "You are a transport analytics copilot. Use only provided context values. Be concise and actionable.";

  function extractTextFromResponses(data) {
    if (!data) return "";
    if (typeof data.output_text === "string" && data.output_text.trim()) {
      return data.output_text.trim();
    }
    const chunks = [];
    const outputs = Array.isArray(data.output) ? data.output : [];
    for (const out of outputs) {
      const content = Array.isArray(out?.content) ? out.content : [];
      for (const c of content) {
        if (typeof c?.text === "string" && c.text.trim()) chunks.push(c.text.trim());
      }
    }
    return chunks.join("\n").trim();
  }

  async function callResponses() {
    const res = await fetch("https://api.openai.com/v1/responses", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${key}`
      },
      body: JSON.stringify({
        model,
        instructions,
        input: prompt
      })
    });
    if (!res.ok) {
      const errText = await res.text();
      throw new Error(`responses ${res.status}: ${errText.slice(0, 600)}`);
    }
    const data = await res.json();
    return extractTextFromResponses(data);
  }

  async function callChatCompletionsFallback() {
    const res = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${key}`
      },
      body: JSON.stringify({
        model,
        messages: [
          { role: "system", content: instructions },
          { role: "user", content: prompt }
        ]
      })
    });
    if (!res.ok) {
      const errText = await res.text();
      throw new Error(`chat.completions ${res.status}: ${errText.slice(0, 600)}`);
    }
    const data = await res.json();
    const msg = data?.choices?.[0]?.message?.content;
    const text = typeof msg === "string" ? msg : Array.isArray(msg) ? msg.map((x) => x?.text || "").join(" ") : "";
    return text.trim();
  }

  try {
    const text = await callResponses();
    if (text) return { text, error: null };
    throw new Error("OpenAI empty response");
  } catch (e) {
    const primaryErr = String(e.message || e);
    try {
      const text = await callChatCompletionsFallback();
      if (text) return { text, error: null };
      return { text: null, error: `OpenAI fallback empty response (primary error: ${primaryErr})` };
    } catch (e2) {
      return {
        text: null,
        error: `OpenAI request failed (${primaryErr}); fallback failed: ${String(e2.message || e2)}`
      };
    }
  }
}

export async function answerAnalyticsQuestion({ corridorId, question }) {
  const q = (question || "").trim();
  if (!q) return { answer: "Please enter a question.", modelUsed: "heuristic" };

  const [latest, trend, cameras] = await Promise.all([
    latestStats(corridorId),
    trendStats(corridorId, 24),
    topCameras(corridorId, 5)
  ]);
  if (!latest) return { answer: "No run data yet for this corridor.", modelUsed: "heuristic" };

  const context = { latest, trend, top_cameras: cameras };
  const llm = await maybeAskOpenAI(q, context);
  if (llm?.text) return { answer: llm.text, modelUsed: "openai", aiError: null };

  let answer = "";
  if (containsAny(q, ["predict", "forecast", "next"])) {
    answer = `Next-interval forecast: p50=${Number(latest.predicted_next_score_p50 || 0).toFixed(1)}, p90=${Number(
      latest.predicted_next_score_p90 || 0
    ).toFixed(1)}, confidence=${Number(latest.prediction_confidence || 0).toFixed(1)}%.`;
  } else if (containsAny(q, ["trend", "average", "last 24", "today"])) {
    answer = `Last 24h: avg score=${Number(trend.avg_score || 0).toFixed(1)}, max score=${Number(
      trend.max_score || 0
    ).toFixed(1)}, points=${Number(trend.points || 0)}.`;
  } else if (containsAny(q, ["camera", "hotspot", "where"])) {
    const top = cameras.map((c) => `${c.camera_location} (${Number(c.avg_image_score).toFixed(1)})`).join("; ");
    answer = top ? `Top camera hotspots (6h): ${top}.` : "No recent camera hotspot data.";
  } else if (containsAny(q, ["weather", "rain", "snow", "storm", "wind", "radar"])) {
    const weather = latest.raw_json?.weather || {};
    answer =
      `Weather risk=${Number(latest.weather_risk_score || 0).toFixed(1)} (component=${Number(
        latest.weather_component || 0
      ).toFixed(1)}). ` +
      `Conditions="${weather.condition_text || "unknown"}". ` +
      `Alerts=${Number(weather.alerts_count || 0)}, precip=${Number(weather.precip_probability_pct || 0).toFixed(
        0
      )}%, wind=${Number(weather.wind_mph || 0).toFixed(1)} mph.`;
  } else {
    answer =
      `Latest score=${Number(latest.fused_score || 0).toFixed(1)} ` +
      `(alert=${latest.alert_state}, incidents=${latest.incidents_count}, closures=${latest.closures_count}, weather=${Number(
        latest.weather_risk_score || 0
      ).toFixed(1)}). ` +
      `Forecast p50=${Number(latest.predicted_next_score_p50 || 0).toFixed(1)}.`;
  }
  return { answer, modelUsed: "heuristic", aiError: llm?.error || "OpenAI unavailable" };
}
