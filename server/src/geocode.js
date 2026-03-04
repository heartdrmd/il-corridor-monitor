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

