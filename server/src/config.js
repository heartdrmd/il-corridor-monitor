export const config = {
  port: Number(process.env.PORT || 8080),
  pollSeconds: Number(process.env.POLL_SECONDS || 300),
  sampleLimit: Number(process.env.SAMPLE_LIMIT || 40),
  baselineAlpha: Number(process.env.BASELINE_ALPHA || 0.2),
  dbUrl: process.env.DATABASE_URL || "postgres://postgres:postgres@localhost:5432/corridor_monitor"
};

export const feeds = {
  traffic: "https://services2.arcgis.com/aIrBD8yn1TDTEXoz/arcgis/rest/services/IL_LiveTraffic_QuadCities_Vw/FeatureServer/0/query",
  incidents: "https://services2.arcgis.com/aIrBD8yn1TDTEXoz/arcgis/rest/services/Illinois_Roadway_Incidents/FeatureServer/0/query",
  closures: "https://services2.arcgis.com/aIrBD8yn1TDTEXoz/arcgis/rest/services/ClosureIncidents/FeatureServer/0/query",
  cameras: "https://services2.arcgis.com/aIrBD8yn1TDTEXoz/arcgis/rest/services/TrafficCamerasTM_Public/FeatureServer/0/query"
};
