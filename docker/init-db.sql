-- Datenbank-Initialisierung für Sensor Data Streaming Pipeline
-- Wird automatisch beim ersten Start von PostgreSQL ausgeführt

-- Tabelle für aggregierte Sensor-Daten
CREATE TABLE IF NOT EXISTS sensor_aggregates (
    id SERIAL PRIMARY KEY,
    sensor_id VARCHAR(50) NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    count_readings INTEGER NOT NULL,
    avg_temperature DOUBLE PRECISION,
    std_temperature DOUBLE PRECISION,
    min_temperature DOUBLE PRECISION,
    max_temperature DOUBLE PRECISION,
    avg_humidity DOUBLE PRECISION,
    min_humidity DOUBLE PRECISION,
    max_humidity DOUBLE PRECISION,
    avg_pressure DOUBLE PRECISION,
    min_pressure DOUBLE PRECISION,
    max_pressure DOUBLE PRECISION,
    outlier_ratio DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indizes für bessere Query-Performance
CREATE INDEX IF NOT EXISTS idx_sensor_aggregates_sensor_id 
    ON sensor_aggregates(sensor_id);

CREATE INDEX IF NOT EXISTS idx_sensor_aggregates_window_end 
    ON sensor_aggregates(window_end);

CREATE INDEX IF NOT EXISTS idx_sensor_aggregates_window_start 
    ON sensor_aggregates(window_start);

-- Kommentar zur Tabelle
COMMENT ON TABLE sensor_aggregates IS 
    'Aggregierte Sensor-Daten mit 1-Minuten-Tumbling-Windows und Z-Score Outlier Detection';

-- Kommentare zu wichtigen Spalten
COMMENT ON COLUMN sensor_aggregates.window_start IS 
    'Start des Aggregationsfensters (1 Minute)';
COMMENT ON COLUMN sensor_aggregates.window_end IS 
    'Ende des Aggregationsfensters (1 Minute)';
COMMENT ON COLUMN sensor_aggregates.outlier_ratio IS 
    'Anteil der Ausreißer (Z-Score > 2.0) im Fenster (0.0 - 1.0)';