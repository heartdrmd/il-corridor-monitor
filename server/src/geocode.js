export async function geocodeCity(query) {
  const q = (query || "").trim();
  if (!q) return null;
  const url = new URL("https://nominatim.openstreetmap.org/search");
  url.searchParams.set("q", q);
  url.searchParams.set("format", "jsonv2");
  url.searchParams.set("limit", "1");
  url.searchParams.set("countrycodes", "us");

  const res = await fetch(url, {
    headers: {
      "User-Agent": "il-corridor-monitor/0.1 (traffic analytics)"
    }
  });
  if (!res.ok) return null;
  const data = await res.json();
  if (!Array.isArray(data) || data.length === 0) return null;
  const hit = data[0];
  return {
    label: hit.display_name,
    lat: Number(hit.lat),
    lon: Number(hit.lon),
    bbox: hit.boundingbox?.map(Number) || null
  };
}

export function bboxFromCenter(lat, lon, radiusKm = 20) {
  const dLat = radiusKm / 111;
  const dLon = radiusKm / (111 * Math.cos((lat * Math.PI) / 180));
  return {
    xmin: lon - dLon,
    ymin: lat - dLat,
    xmax: lon + dLon,
    ymax: lat + dLat
  };
}

function haversineKm(aLat, aLon, bLat, bLon) {
  const toRad = (d) => (d * Math.PI) / 180;
  const dLat = toRad(bLat - aLat);
  const dLon = toRad(bLon - aLon);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(aLat)) * Math.cos(toRad(bLat)) * Math.sin(dLon / 2) ** 2;
  return 6371 * (2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)));
}

function interpolate(a, b, t) {
  return {
    lat: a.lat + (b.lat - a.lat) * t,
    lon: a.lon + (b.lon - a.lon) * t
  };
}

export async function fetchRoadRoutePoints(fromLat, fromLon, toLat, toLon) {
  const url = new URL(
    `https://router.project-osrm.org/route/v1/driving/${fromLon},${fromLat};${toLon},${toLat}`
  );
  url.searchParams.set("overview", "full");
  url.searchParams.set("geometries", "geojson");
  url.searchParams.set("steps", "false");
  try {
    const res = await fetch(url);
    if (!res.ok) return null;
    const data = await res.json();
    const coords = data?.routes?.[0]?.geometry?.coordinates;
    if (!Array.isArray(coords) || coords.length < 2) return null;
    return coords.map((c) => ({ lon: Number(c[0]), lat: Number(c[1]) }));
  } catch {
    return null;
  }
}

export function straightLinePoints(fromLat, fromLon, toLat, toLon, segments = 24) {
  const out = [];
  for (let i = 0; i <= segments; i++) {
    const t = i / segments;
    out.push({
      lat: fromLat + (toLat - fromLat) * t,
      lon: fromLon + (toLon - fromLon) * t
    });
  }
  return out;
}

export function samplePointsAlongPolyline(points, spacingKm = 16) {
  if (!Array.isArray(points) || points.length < 2) return [];
  const out = [{ ...points[0], distance_km: 0 }];
  let carry = 0;
  let traveled = 0;
  for (let i = 1; i < points.length; i++) {
    let a = points[i - 1];
    const b = points[i];
    let segKm = haversineKm(a.lat, a.lon, b.lat, b.lon);
    if (segKm <= 0) continue;
    while (carry + segKm >= spacingKm) {
      const need = spacingKm - carry;
      const t = need / segKm;
      const p = interpolate(a, b, t);
      traveled += need;
      out.push({ ...p, distance_km: traveled });
      a = p;
      segKm = haversineKm(a.lat, a.lon, b.lat, b.lon);
      carry = 0;
    }
    carry += segKm;
    traveled += segKm;
  }
  const last = points[points.length - 1];
  const maybeLast = out[out.length - 1];
  if (!maybeLast || haversineKm(maybeLast.lat, maybeLast.lon, last.lat, last.lon) > 0.5) {
    out.push({ ...last, distance_km: traveled });
  }
  return out;
}
