import { feeds } from "./config.js";

const CA_DISTRICTS_ALL = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
const CA_TT_DISTRICTS = new Set([3, 8, 11, 12]);
const responseCache = new Map();

function clamp(v, lo, hi) {
  return Math.max(lo, Math.min(hi, v));
}

function pad2(n) {
  return String(Number(n)).padStart(2, "0");
}

function arcgisParams(base = {}) {
  return new URLSearchParams({
    where: "1=1",
    f: "pjson",
    ...base
  });
}

async function arcgisQuery(url, params, timeoutMs = 15000) {
  const res = await fetch(`${url}?${params.toString()}`, {
    signal: AbortSignal.timeout(timeoutMs)
  });
  if (!res.ok) throw new Error(`ArcGIS query failed: ${res.status}`);
  return res.json();
}

function bboxString(c) {
  return `${c.xmin},${c.ymin},${c.xmax},${c.ymax}`;
}

function pointInBbox(lat, lon, bbox) {
  if (!bbox) return true;
  return Number(lon) >= Number(bbox.xmin) &&
    Number(lon) <= Number(bbox.xmax) &&
    Number(lat) >= Number(bbox.ymin) &&
    Number(lat) <= Number(bbox.ymax);
}

function featurePoint(feature) {
  if (!feature) return null;
  const attrs = feature.attributes || feature;
  const g = feature.geometry || {};
  const x = Number(
    attrs.x ??
      attrs.X ??
      attrs.lon ??
      attrs.LON ??
      attrs.Longitude ??
      attrs.LONGITUDE ??
      g.x ??
      g.X
  );
  const y = Number(
    attrs.y ??
      attrs.Y ??
      attrs.lat ??
      attrs.LAT ??
      attrs.Latitude ??
      attrs.LATITUDE ??
      g.y ??
      g.Y
  );
  if (Number.isFinite(y) && Number.isFinite(x)) return { lat: y, lon: x };

  const paths = g.paths;
  if (Array.isArray(paths) && paths.length && Array.isArray(paths[0]) && paths[0].length) {
    const first = paths[0][0];
    const last = paths[0][paths[0].length - 1];
    if (Array.isArray(first) && Array.isArray(last)) {
      const lon = (Number(first[0]) + Number(last[0])) / 2;
      const lat = (Number(first[1]) + Number(last[1])) / 2;
      if (Number.isFinite(lat) && Number.isFinite(lon)) return { lat, lon };
    }
  }
  return null;
}

function normalizeProfileId(raw) {
  const v = String(raw || "").trim().toLowerCase();
  if (v === "ca" || v === "ca_cwwp2" || v === "california") return "ca_cwwp2";
  return "il_arcgis";
}

export function listFeedProfiles() {
  return [
    {
      id: "il_arcgis",
      name: "Illinois ArcGIS",
      description: "IDOT ArcGIS feeds (traffic segments, incidents, closures, cameras).",
      coverage: "Primary for Illinois"
    },
    {
      id: "ca_cwwp2",
      name: "California CWWP2",
      description: "Caltrans CWWP2 feeds (CCTV, Lane Closures, Chain Controls, Travel Times).",
      coverage: "Primary for California"
    }
  ];
}

export function normalizeCorridorFeedProfile(corridor) {
  return normalizeProfileId(corridor?.feed_profile);
}

function parseDistrictList(raw) {
  if (Array.isArray(raw)) {
    const out = raw.map((x) => Number(x)).filter((n) => Number.isInteger(n) && n >= 1 && n <= 12);
    return [...new Set(out)];
  }
  const txt = String(raw || "").trim();
  if (!txt) return [];
  const out = txt
    .split(",")
    .map((x) => Number(x.trim()))
    .filter((n) => Number.isInteger(n) && n >= 1 && n <= 12);
  return [...new Set(out)];
}

function corridorCaDistricts(corridor) {
  const cfg = corridor?.feed_config && typeof corridor.feed_config === "object" ? corridor.feed_config : {};
  const fromCfg = parseDistrictList(cfg.districts || cfg.ca_districts || cfg.district_list || "");
  return fromCfg.length ? fromCfg : [...CA_DISTRICTS_ALL];
}

async function fetchJsonCached(url, { ttlMs = 120000, timeoutMs = 15000 } = {}) {
  const now = Date.now();
  const hit = responseCache.get(url);
  if (hit && hit.expiresAt > now) return hit.data;
  try {
    const res = await fetch(url, {
      signal: AbortSignal.timeout(timeoutMs),
      headers: {
        Accept: "application/json,text/plain,*/*",
        "User-Agent": "il-corridor-monitor/1.0"
      }
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    responseCache.set(url, { expiresAt: now + ttlMs, data });
    return data;
  } catch (e) {
    if (hit?.data) return hit.data;
    throw e;
  }
}

async function fetchJsonMaybe(url, opts = {}) {
  try {
    return await fetchJsonCached(url, opts);
  } catch {
    return null;
  }
}

function caUrl(kind, district) {
  const d = Number(district);
  const dd = pad2(d);
  if (kind === "cctv") return `https://cwwp2.dot.ca.gov/data/d${d}/cctv/cctvStatusD${dd}.json`;
  if (kind === "tt") return `https://cwwp2.dot.ca.gov/data/d${d}/tt/ttStatusD${dd}.json`;
  if (kind === "lcs") return `https://cwwp2.dot.ca.gov/data/d${d}/lcs/lcsStatusD${dd}.json`;
  if (kind === "cc") return `https://cwwp2.dot.ca.gov/data/d${d}/cc/ccStatusD${dd}.json`;
  return "";
}

function cwwpRows(payload) {
  return Array.isArray(payload?.data) ? payload.data : [];
}

function parseCaCameraFeature(entry, district) {
  const cctv = entry?.cctv || entry;
  const loc = cctv?.location || {};
  const lat = Number(loc.latitude);
  const lon = Number(loc.longitude);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  const idx = Number(cctv?.index || 0);
  const objectId = Number.isFinite(idx) && idx > 0 ? district * 100000 + idx : Date.now() + Math.floor(Math.random() * 1000);
  const inService = String(cctv?.inService || "").toLowerCase() === "true";
  const snapshot = String(cctv?.imageData?.static?.currentImageURL || "").replace(/\\\//g, "/");
  const epochRaw = Number(cctv?.recordTimestamp?.recordEpoch || 0);
  const epochMs = epochRaw > 1e12 ? epochRaw : epochRaw > 0 ? epochRaw * 1000 : NaN;
  const ageMinutes = Number.isFinite(epochMs) ? Math.max(0, (Date.now() - epochMs) / 60000) : null;
  const tooOld = !inService || !snapshot;
  return {
    attributes: {
      OBJECTID: objectId,
      CameraLocation: String(loc.locationName || cctv?.locationName || `D${district} camera ${idx}`),
      CameraDirection: String(loc.direction || ""),
      SnapShot: snapshot,
      AgeInMinutes: Number.isFinite(ageMinutes) ? Number(ageMinutes.toFixed(2)) : null,
      TooOld: tooOld ? "true" : "false",
      district: district,
      county: String(loc.county || ""),
      route: String(loc.route || "")
    },
    geometry: { x: lon, y: lat }
  };
}

function parseCaTtFeature(entry, district) {
  const tt = entry?.tt || entry;
  const seg = Array.isArray(tt?.segments?.segment) ? tt.segments.segment[0] : tt?.segments?.segment;
  const begin = seg?.beginSegment || tt?.segments?.beginSegment || {};
  const end = seg?.endSegment || tt?.segments?.endSegment || {};
  const bLat = Number(begin.latitude);
  const bLon = Number(begin.longitude);
  const eLat = Number(end.latitude);
  const eLon = Number(end.longitude);
  if (![bLat, bLon, eLat, eLon].every((n) => Number.isFinite(n))) return null;

  const routeLengthMiles = Number(tt?.location?.routeLength || tt?.location?.segmentLength || tt?.routeLength || 0);
  const currentMin = Number(tt?.currentTravelTime || tt?.travelTime || tt?.averageTime || 0);
  const freeFlowMin = Number(tt?.freeFlowTravelTime || tt?.averageTravelTime || tt?.averageTime || currentMin);
  if (!Number.isFinite(routeLengthMiles) || routeLengthMiles <= 0) return null;
  if (!Number.isFinite(currentMin) || currentMin <= 0) return null;
  if (!Number.isFinite(freeFlowMin) || freeFlowMin <= 0) return null;

  const speed = routeLengthMiles / (currentMin / 60);
  const speedFF = routeLengthMiles / (freeFlowMin / 60);
  if (!Number.isFinite(speed) || !Number.isFinite(speedFF) || speedFF <= 0 || speed < 0) return null;

  const delayPct = clamp(((speedFF - speed) / speedFF) * 100, 0, 100);
  return {
    attributes: {
      OBJECTID: Number(tt?.index || 0) + district * 100000,
      SPEED: Number(speed.toFixed(2)),
      SPEED_FF: Number(speedFF.toFixed(2)),
      JAM_FACTOR: Number((delayPct / 10).toFixed(2)),
      Direction: String(begin.direction || ""),
      district: district,
      route: String(tt?.location?.route || "")
    },
    geometry: {
      paths: [
        [
          [bLon, bLat],
          [eLon, eLat]
        ]
      ]
    }
  };
}

function parseNaiveDateTimeMs(dateText, timeText) {
  const d = String(dateText || "").trim();
  const t = String(timeText || "").trim();
  if (!d || !t) return null;
  const m = d.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  const tm = t.match(/^(\d{1,2}):(\d{2})$/);
  if (!m || !tm) return null;
  const mm = String(m[1]).padStart(2, "0");
  const dd = String(m[2]).padStart(2, "0");
  const yyyy = String(m[3]);
  const hh = String(tm[1]).padStart(2, "0");
  const mi = String(tm[2]).padStart(2, "0");
  const iso = `${yyyy}-${mm}-${dd}T${hh}:${mi}:00`;
  const ms = Date.parse(iso);
  return Number.isFinite(ms) ? ms : null;
}

function closureWeightFromSchedule(lcs) {
  const startMs = parseNaiveDateTimeMs(lcs?.schedule?.startDate, lcs?.schedule?.startTime);
  const endMs = parseNaiveDateTimeMs(lcs?.schedule?.endDate, lcs?.schedule?.endTime);
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) return 1;
  const now = Date.now();
  if (startMs <= now && now <= endMs) return 1;
  if (startMs > now) {
    const dt = startMs - now;
    if (dt <= 6 * 3600 * 1000) return 0.5;
    if (dt <= 24 * 3600 * 1000) return 0.25;
    return 0;
  }
  if (now - endMs <= 2 * 3600 * 1000) return 0.2;
  return 0;
}

function parseCaLcsClosureFeature(entry, district) {
  const lcs = entry?.lcs || entry;
  const seg = Array.isArray(lcs?.segments?.segment) ? lcs.segments.segment[0] : lcs?.segments?.segment;
  const begin = seg?.beginSegment || {};
  const end = seg?.endSegment || {};
  const bLat = Number(begin.latitude);
  const bLon = Number(begin.longitude);
  const eLat = Number(end.latitude);
  const eLon = Number(end.longitude);
  const locLat = Number(lcs?.location?.latitude);
  const locLon = Number(lcs?.location?.longitude);
  const lat = Number.isFinite(bLat) && Number.isFinite(eLat) ? (bLat + eLat) / 2 : locLat;
  const lon = Number.isFinite(bLon) && Number.isFinite(eLon) ? (bLon + eLon) / 2 : locLon;
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  const baseWeight = closureWeightFromSchedule(lcs);
  if (baseWeight <= 0) return null;
  const closureType = String(lcs?.details?.closureType || "");
  const fullBoost = /full/i.test(closureType) ? 1.2 : 1;
  const weight = clamp(baseWeight * fullBoost, 0.1, 2);

  return {
    attributes: {
      OBJECTID: Number(lcs?.index || 0) + district * 100000,
      Direction: String(begin.direction || ""),
      closureType,
      lanesClosed: String(lcs?.details?.lanesClosed || ""),
      route: String(lcs?.location?.route || ""),
      district,
      __weight: weight
    },
    geometry: { x: lon, y: lat }
  };
}

function parseCaCcIncidentFeature(entry, district) {
  const cc = entry?.cc || entry;
  const seg = Array.isArray(cc?.segments?.segment) ? cc.segments.segment[0] : cc?.segments?.segment;
  const begin = seg?.beginSegment || {};
  const end = seg?.endSegment || {};
  const bLat = Number(begin.latitude);
  const bLon = Number(begin.longitude);
  const eLat = Number(end.latitude);
  const eLon = Number(end.longitude);
  const locLat = Number(cc?.location?.latitude);
  const locLon = Number(cc?.location?.longitude);
  const lat = Number.isFinite(bLat) && Number.isFinite(eLat) ? (bLat + eLat) / 2 : locLat;
  const lon = Number.isFinite(bLon) && Number.isFinite(eLon) ? (bLon + eLon) / 2 : locLon;
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;

  const restriction = String(cc?.details?.restriction || "");
  const roadCondition = String(cc?.details?.roadCondition || "");
  const baseWeight = /no restrictions?/i.test(restriction) ? 0.4 : 1;
  return {
    attributes: {
      OBJECTID: Number(cc?.index || 0) + district * 100000,
      Direction: String(begin.direction || ""),
      route: String(cc?.location?.route || ""),
      restriction,
      roadCondition,
      district,
      __weight: baseWeight
    },
    geometry: { x: lon, y: lat }
  };
}

function filterFeaturesByBbox(features, bbox) {
  if (!bbox) return features;
  return (features || []).filter((f) => {
    const pt = featurePoint(f);
    return pt ? pointInBbox(pt.lat, pt.lon, bbox) : false;
  });
}

async function fetchIlArcgisSnapshot(corridor, opts = {}) {
  const bbox = opts.bboxOverride || corridor;
  const timeoutMs = Math.max(3000, Number(opts.timeoutMs || 15000));
  const needTraffic = opts.needTraffic !== false;
  const needIncidents = opts.needIncidents !== false;
  const needClosures = opts.needClosures !== false;
  const needCameras = opts.needCameras !== false;
  const common = {
    geometry: bboxString(bbox),
    geometryType: "esriGeometryEnvelope",
    inSR: "4326",
    spatialRel: "esriSpatialRelIntersects"
  };
  const [traffic, incidents, closures, cameras] = await Promise.all([
    needTraffic
      ? arcgisQuery(
          feeds.traffic,
          arcgisParams({ ...common, outFields: "SPEED,SPEED_FF,JAM_FACTOR", outSR: "4326", returnGeometry: "true" }),
          timeoutMs
        )
      : { features: [] },
    needIncidents
      ? arcgisQuery(
          feeds.incidents,
          arcgisParams({ ...common, outFields: "*", outSR: "4326", returnGeometry: "true" }),
          timeoutMs
        )
      : { features: [] },
    needClosures
      ? arcgisQuery(
          feeds.closures,
          arcgisParams({ ...common, outFields: "*", outSR: "4326", returnGeometry: "true" }),
          timeoutMs
        )
      : { features: [] },
    needCameras
      ? arcgisQuery(
          feeds.cameras,
          arcgisParams({
            ...common,
            outFields: "OBJECTID,CameraLocation,CameraDirection,SnapShot,AgeInMinutes,TooOld",
            outSR: "4326",
            returnGeometry: "true"
          }),
          timeoutMs
        )
      : { features: [] }
  ]);
  return {
    provider: "il_arcgis",
    trafficFeatures: Array.isArray(traffic?.features) ? traffic.features : [],
    incidentFeatures: Array.isArray(incidents?.features) ? incidents.features : [],
    closureFeatures: Array.isArray(closures?.features) ? closures.features : [],
    cameraFeatures: Array.isArray(cameras?.features) ? cameras.features : [],
    counts: {
      traffic_raw: Array.isArray(traffic?.features) ? traffic.features.length : 0,
      incidents_raw: Array.isArray(incidents?.features) ? incidents.features.length : 0,
      closures_raw: Array.isArray(closures?.features) ? closures.features.length : 0,
      cameras_raw: Array.isArray(cameras?.features) ? cameras.features.length : 0
    },
    meta: {
      source: "arcgis",
      districts: []
    }
  };
}

async function fetchCaCwwp2Snapshot(corridor, opts = {}) {
  const bbox = opts.bboxOverride || corridor;
  const timeoutMs = Math.max(3000, Number(opts.timeoutMs || 15000));
  const needTraffic = opts.needTraffic !== false;
  const needIncidents = opts.needIncidents !== false;
  const needClosures = opts.needClosures !== false;
  const needCameras = opts.needCameras !== false;
  const districts = corridorCaDistricts(corridor);
  const failures = { cctv: [], tt: [], lcs: [], cc: [] };
  const ttlMs = 120000;

  const cameraFeatures = [];
  const trafficFeatures = [];
  const closureFeatures = [];
  const incidentFeatures = [];

  const jobs = [];
  for (const d of districts) {
    if (needCameras) jobs.push({ kind: "cctv", district: d });
    if (needClosures) jobs.push({ kind: "lcs", district: d });
    if (needIncidents) jobs.push({ kind: "cc", district: d });
    if (needTraffic && CA_TT_DISTRICTS.has(d)) jobs.push({ kind: "tt", district: d });
  }

  const settled = await Promise.all(
    jobs.map(async (j) => {
      const url = caUrl(j.kind, j.district);
      const payload = await fetchJsonMaybe(url, { timeoutMs, ttlMs });
      return { ...j, payload };
    })
  );

  for (const row of settled) {
    const { kind, district, payload } = row;
    if (!payload) {
      failures[kind].push(district);
      continue;
    }
    const records = cwwpRows(payload);
    if (kind === "cctv") {
      for (const rec of records) {
        const f = parseCaCameraFeature(rec, district);
        if (f) cameraFeatures.push(f);
      }
      continue;
    }
    if (kind === "tt") {
      for (const rec of records) {
        const f = parseCaTtFeature(rec, district);
        if (f) trafficFeatures.push(f);
      }
      continue;
    }
    if (kind === "lcs") {
      for (const rec of records) {
        const f = parseCaLcsClosureFeature(rec, district);
        if (f) closureFeatures.push(f);
      }
      continue;
    }
    if (kind === "cc") {
      for (const rec of records) {
        const f = parseCaCcIncidentFeature(rec, district);
        if (f) incidentFeatures.push(f);
      }
    }
  }

  const keptTraffic = filterFeaturesByBbox(trafficFeatures, bbox);
  const keptIncidents = filterFeaturesByBbox(incidentFeatures, bbox);
  const keptClosures = filterFeaturesByBbox(closureFeatures, bbox);
  const keptCameras = filterFeaturesByBbox(cameraFeatures, bbox);

  return {
    provider: "ca_cwwp2",
    trafficFeatures: keptTraffic,
    incidentFeatures: keptIncidents,
    closureFeatures: keptClosures,
    cameraFeatures: keptCameras,
    counts: {
      traffic_raw: trafficFeatures.length,
      incidents_raw: incidentFeatures.length,
      closures_raw: closureFeatures.length,
      cameras_raw: cameraFeatures.length,
      traffic_kept: keptTraffic.length,
      incidents_kept: keptIncidents.length,
      closures_kept: keptClosures.length,
      cameras_kept: keptCameras.length
    },
    meta: {
      source: "cwwp2",
      districts,
      failures
    }
  };
}

export async function fetchNormalizedFeeds(corridor, opts = {}) {
  const profile = normalizeCorridorFeedProfile(corridor);
  if (profile === "ca_cwwp2") {
    return fetchCaCwwp2Snapshot(corridor, opts);
  }
  return fetchIlArcgisSnapshot(corridor, opts);
}
