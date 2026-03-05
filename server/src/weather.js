const NWS_USER_AGENT = process.env.NWS_USER_AGENT || "il-corridor-monitor/1.0 (contact: admin@example.com)";
const LIVE_CACHE_TTL_MS = 2 * 60 * 1000;
const RADAR_TIME_CACHE_TTL_MS = 5 * 60 * 1000;
const RADAR_WMS_BASE_URL = "https://opengeo.ncep.noaa.gov/geoserver/ows";
const RADAR_WMS_LAYER = "conus:conus_bref_qcd";
const liveCache = new Map();
const radarTimeCache = new Map();

function clamp(v, min = 0, max = 100) {
  return Math.max(min, Math.min(max, v));
}

function toNum(v, fallback = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function metersToMiles(meters) {
  if (!Number.isFinite(meters)) return null;
  return meters / 1609.344;
}

function mpsToMph(mps) {
  if (!Number.isFinite(mps)) return null;
  return mps * 2.236936;
}

function parseWindSpeedTextMph(text) {
  const s = String(text || "");
  const nums = s.match(/\d+(\.\d+)?/g)?.map(Number) || [];
  if (!nums.length) return null;
  return Math.max(...nums);
}

function corridorCenter(corridor) {
  const xmin = toNum(corridor?.xmin, 0);
  const xmax = toNum(corridor?.xmax, 0);
  const ymin = toNum(corridor?.ymin, 0);
  const ymax = toNum(corridor?.ymax, 0);
  return {
    lat: (ymin + ymax) / 2,
    lon: (xmin + xmax) / 2
  };
}

function paddedBbox(corridor) {
  const xmin = toNum(corridor?.xmin, -90);
  const xmax = toNum(corridor?.xmax, -88);
  const ymin = toNum(corridor?.ymin, 39);
  const ymax = toNum(corridor?.ymax, 41);
  const w = Math.abs(xmax - xmin);
  const h = Math.abs(ymax - ymin);
  // Wider context helps orientation when users need landmarks.
  const padX = Math.max(0.22, w * 0.28);
  const padY = Math.max(0.22, h * 0.28);
  return {
    xmin: xmin - padX,
    ymin: ymin - padY,
    xmax: xmax + padX,
    ymax: ymax + padY
  };
}

export function buildRadarImageUrl(corridor, opts = {}) {
  const b = paddedBbox(corridor);
  const width = Math.max(400, Math.min(1600, Number(opts.width || 980)));
  const height = Math.max(250, Math.min(1000, Number(opts.height || 560)));
  const params = new URLSearchParams({
    service: "WMS",
    version: "1.1.1",
    request: "GetMap",
    // Add a base geopolitical layer so low-precip scenes are still visible.
    layers: "geopolitical,conus:conus_bref_qcd",
    styles: "",
    format: "image/png",
    transparent: "false",
    bgcolor: "0x0b1d31",
    srs: "EPSG:4326",
    bbox: `${b.xmin},${b.ymin},${b.xmax},${b.ymax}`,
    width: String(width),
    height: String(height)
  });
  return `${RADAR_WMS_BASE_URL}?${params.toString()}`;
}

function parseTimeExtentForLayer(xml, layerName) {
  const nameTag = `<Name>${layerName}</Name>`;
  const layerStart = xml.indexOf(nameTag);
  if (layerStart < 0) return { times: [], defaultTime: "" };
  const nextLayer = xml.indexOf("<Layer", layerStart + nameTag.length);
  const layerEnd = nextLayer > layerStart ? nextLayer : xml.length;
  const section = xml.slice(layerStart, layerEnd);

  const extentMatch = section.match(/<Extent[^>]*name="time"[^>]*>([^<]+)<\/Extent>/i);
  const defaultMatch = section.match(/<Extent[^>]*name="time"[^>]*default="([^"]+)"/i);
  if (!extentMatch) return { times: [], defaultTime: "" };

  const times = String(extentMatch[1] || "")
    .split(",")
    .map((t) => t.trim())
    .filter(Boolean)
    .map((t) => {
      const d = new Date(t);
      return Number.isFinite(d.getTime()) ? d.toISOString() : t;
    });
  const defaultTimeRaw = String(defaultMatch?.[1] || "").trim();
  const defaultParsed = defaultTimeRaw ? new Date(defaultTimeRaw) : null;
  const defaultTime =
    defaultParsed && Number.isFinite(defaultParsed.getTime())
      ? defaultParsed.toISOString()
      : times.length
        ? times[times.length - 1]
        : "";
  return { times, defaultTime };
}

async function fetchRadarFrameTimes(layerName = RADAR_WMS_LAYER) {
  const key = layerName;
  const now = Date.now();
  const cached = radarTimeCache.get(key);
  if (cached && now - cached.ts <= RADAR_TIME_CACHE_TTL_MS) {
    return cached.value;
  }

  const url = `${RADAR_WMS_BASE_URL}?service=WMS&version=1.1.1&request=GetCapabilities`;
  const res = await fetch(url, {
    headers: {
      "User-Agent": NWS_USER_AGENT
    }
  });
  if (!res.ok) {
    if (cached) return cached.value;
    throw new Error(`Radar capabilities HTTP ${res.status}`);
  }
  const xml = await res.text();
  const parsed = parseTimeExtentForLayer(xml, layerName);
  radarTimeCache.set(key, { ts: now, value: parsed });
  return parsed;
}

function buildRadarStationUrl(radarStation) {
  if (!radarStation) return "";
  return `https://radar.weather.gov/station/${encodeURIComponent(String(radarStation).trim())}/standard`;
}

async function nwsGet(url) {
  const res = await fetch(url, {
    headers: {
      Accept: "application/geo+json",
      "User-Agent": NWS_USER_AGENT
    }
  });
  if (!res.ok) {
    throw new Error(`NWS ${res.status} for ${url}`);
  }
  return res.json();
}

function summarizeAlerts(features = []) {
  let severeCount = 0;
  let totalScore = 0;
  for (const f of features) {
    const p = f?.properties || {};
    const event = String(p.event || "").toLowerCase();
    const severity = String(p.severity || "").toLowerCase();

    if (severity === "extreme") {
      totalScore += 65;
      severeCount += 1;
    } else if (severity === "severe") {
      totalScore += 48;
      severeCount += 1;
    } else if (severity === "moderate") {
      totalScore += 28;
    } else {
      totalScore += 12;
    }

    if (event.includes("warning")) totalScore += 16;
    else if (event.includes("watch")) totalScore += 10;
    else if (event.includes("advisory")) totalScore += 6;
  }
  return {
    alertsCount: features.length,
    severeCount,
    risk: clamp(totalScore, 0, 100)
  };
}

function keywordBoost(text, cfg = {}) {
  const t = String(text || "").toLowerCase();
  let boost = 0;
  if (/(snow|blizzard|sleet|freezing rain|ice storm)/.test(t)) {
    boost += toNum(cfg.weather_text_boost_snow, 18);
  }
  if (/(thunderstorm|tornado|hail|severe storm|squall)/.test(t)) {
    boost += toNum(cfg.weather_text_boost_storm, 14);
  }
  if (/(fog|dense fog|smoke|haze)/.test(t)) {
    boost += toNum(cfg.weather_text_boost_fog, 10);
  }
  if (/(rain|showers|drizzle)/.test(t)) {
    boost += toNum(cfg.weather_text_boost_rain, 6);
  }
  return clamp(boost, 0, 45);
}

function computeRiskParts({ precipProb, windMph, visibilityMiles, alertRisk, severeAlerts, combinedText }, cfg = {}) {
  const precipRisk = clamp(toNum(precipProb, 0), 0, 100);
  const windRefMph = Math.max(5, toNum(cfg.weather_wind_ref_mph, 45));
  const windRisk = clamp((toNum(windMph, 0) / windRefMph) * 100, 0, 100);

  const visRefMiles = Math.max(1, toNum(cfg.weather_visibility_ref_miles, 10));
  const visibilityRisk =
    visibilityMiles == null ? 0 : clamp((1 - Math.min(visibilityMiles, visRefMiles) / visRefMiles) * 100, 0, 100);

  const wP = toNum(cfg.weather_precip_weight, 1.1);
  const wW = toNum(cfg.weather_wind_weight, 0.9);
  const wV = toNum(cfg.weather_visibility_weight, 1.0);
  const wA = toNum(cfg.weather_alert_weight, 1.4);
  const wSum = Math.max(0.0001, wP + wW + wV + wA);

  const blended = (wP * precipRisk + wW * windRisk + wV * visibilityRisk + wA * clamp(alertRisk, 0, 100)) / wSum;
  const textBoost = keywordBoost(combinedText, cfg);
  const majorBonus = severeAlerts > 0 ? toNum(cfg.weather_alert_major_bonus, 12) : 0;
  const weatherRisk = clamp(blended + textBoost + majorBonus, 0, 100);

  return {
    precip_risk: precipRisk,
    wind_risk: windRisk,
    visibility_risk: visibilityRisk,
    alert_risk: clamp(alertRisk, 0, 100),
    text_boost: textBoost,
    major_alert_bonus: majorBonus,
    weather_risk_score: weatherRisk
  };
}

function cacheKeyForCorridor(corridor) {
  const c = corridorCenter(corridor);
  return `${c.lat.toFixed(3)},${c.lon.toFixed(3)}`;
}

export async function fetchCorridorWeather(corridor, cfg = {}, opts = {}) {
  const useCache = opts.useCache !== false;
  const key = cacheKeyForCorridor(corridor);
  const now = Date.now();
  const radarImageUrl = buildRadarImageUrl(corridor);
  const radarFrames = await fetchRadarFrameTimes().catch(() => ({ times: [], defaultTime: "" }));

  if (useCache) {
    const cached = liveCache.get(key);
    if (cached && now - cached.ts <= LIVE_CACHE_TTL_MS) {
      return {
        ...cached.value,
        radar_frame_times: radarFrames.times.slice(-45),
        radar_time_default_utc: radarFrames.defaultTime || cached.value?.radar_time_default_utc || ""
      };
    }
  }

  const center = corridorCenter(corridor);
  const base = {
    source: "nws_forecast_alerts+opengeo_mrms",
    corridor_id: corridor?.id ?? null,
    center_lat: center.lat,
    center_lon: center.lon,
    radar_image_url: radarImageUrl,
    radar_source_url: `${RADAR_WMS_BASE_URL}?service=WMS&request=GetCapabilities`,
    radar_wms_base_url: RADAR_WMS_BASE_URL,
    radar_wms_layer: RADAR_WMS_LAYER,
    radar_time_default_utc: radarFrames.defaultTime || "",
    radar_frame_times: radarFrames.times.slice(-45),
    radar_station: "",
    radar_station_url: "",
    fetched_at: new Date().toISOString()
  };

  try {
    const point = await nwsGet(`https://api.weather.gov/points/${center.lat.toFixed(4)},${center.lon.toFixed(4)}`);
    const props = point?.properties || {};
    const forecastHourlyUrl = String(props.forecastHourly || "");
    const stationsUrl = String(props.observationStations || "");
    const radarStation = String(props.radarStation || "");

    const [forecastHourly, alerts, stationsList] = await Promise.all([
      forecastHourlyUrl ? nwsGet(forecastHourlyUrl) : Promise.resolve(null),
      nwsGet(`https://api.weather.gov/alerts/active?point=${center.lat.toFixed(4)},${center.lon.toFixed(4)}`),
      stationsUrl ? nwsGet(stationsUrl) : Promise.resolve(null)
    ]);

    const firstPeriod = forecastHourly?.properties?.periods?.[0] || {};
    const forecastText = String(firstPeriod.shortForecast || "");
    const precipProb = firstPeriod?.probabilityOfPrecipitation?.value;
    const windMphForecast = parseWindSpeedTextMph(firstPeriod.windSpeed);

    let latestObs = null;
    const stationId = stationsList?.features?.[0]?.properties?.stationIdentifier || stationsList?.features?.[0]?.id;
    if (stationId) {
      const stationPath = String(stationId).startsWith("http")
        ? `${String(stationId).replace(/\/$/, "")}/observations/latest`
        : `https://api.weather.gov/stations/${stationId}/observations/latest`;
      try {
        latestObs = await nwsGet(stationPath);
      } catch {
        latestObs = null;
      }
    }

    const obs = latestObs?.properties || {};
    const obsText = String(obs.textDescription || "");
    const obsTime = obs.timestamp ? new Date(obs.timestamp) : null;
    const obsAgeMinutes = obsTime ? (Date.now() - obsTime.getTime()) / 60000 : null;
    const visibilityMiles = metersToMiles(obs?.visibility?.value);
    const windMphObs = mpsToMph(obs?.windSpeed?.value);
    const windMph = Number.isFinite(windMphObs) ? windMphObs : windMphForecast;

    const alertSummary = summarizeAlerts(alerts?.features || []);
    const text = [forecastText, obsText].filter(Boolean).join(" | ");
    const riskParts = computeRiskParts(
      {
        precipProb,
        windMph,
        visibilityMiles,
        alertRisk: alertSummary.risk,
        severeAlerts: alertSummary.severeCount,
        combinedText: text
      },
      cfg
    );

    const out = {
      ...base,
      available: true,
      radar_station: radarStation,
      radar_station_url: buildRadarStationUrl(radarStation),
      forecast_short: forecastText,
      observation_text: obsText,
      condition_text: text,
      precip_probability_pct: toNum(precipProb, 0),
      wind_mph: toNum(windMph, 0),
      visibility_miles: visibilityMiles == null ? null : Number(visibilityMiles),
      observation_age_minutes: obsAgeMinutes == null ? null : Number(obsAgeMinutes),
      alerts_count: alertSummary.alertsCount,
      severe_alerts_count: alertSummary.severeCount,
      ...riskParts
    };

    if (useCache) {
      liveCache.set(key, { ts: now, value: out });
    }
    return out;
  } catch (e) {
    return {
      ...base,
      available: false,
      weather_risk_score: 0,
      precip_probability_pct: 0,
      wind_mph: 0,
      visibility_miles: null,
      alerts_count: 0,
      severe_alerts_count: 0,
      condition_text: "",
      error: String(e.message || e)
    };
  }
}
