import json
import sys
from typing import Optional, Dict, Tuple, List
from datetime import datetime, timezone, timedelta

from confluent_kafka import Consumer, KafkaError
import psycopg2
from psycopg2.extras import execute_values

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "sensor_readings_raw"
KAFKA_GROUP_ID = "sensor-consumer-group"

PG_HOST = "localhost"
PG_PORT = 5432
PG_DB = "de_db"
PG_USER = "de_user"
PG_PASSWORD = "de_password"

WINDOW_SIZE_SECONDS = 60  # 1-Minuten-Tumbling-Window


def get_pg_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def floor_to_window(ts: datetime) -> datetime:
    """Rundet einen Timestamp auf den Start seines Tumbling-Windows herunter."""
    ts = ts.astimezone(timezone.utc)
    seconds = int(ts.timestamp())
    window_start_sec = seconds - (seconds % WINDOW_SIZE_SECONDS)
    return datetime.fromtimestamp(window_start_sec, tz=timezone.utc)


def parse_message(raw_value: bytes) -> Optional[Tuple[str, datetime, float, float, float]]:
    try:
        data = json.loads(raw_value.decode("utf-8"))
        sensor_id = str(data["sensor_id"])
        ts = datetime.fromisoformat(data["timestamp"])
        temperature = data.get("temperature")
        humidity = data.get("humidity")
        pressure = data.get("pressure")
        return sensor_id, ts, temperature, humidity, pressure
    except Exception as e:
        print(f"Failed to parse message: {e}", file=sys.stderr)
        return None


def is_outlier_temp(temp: float) -> bool:
    if temp is None:
        return False
    return temp < 15.0 or temp > 30.0


def main():
    consumer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([KAFKA_TOPIC])

    conn = get_pg_connection()
    conn.autocommit = False
    cur = conn.cursor()

    # Zustand für laufende Fenster:
    # key: (sensor_id, window_start)
    # value: dict mit running stats
    windows: Dict[Tuple[str, datetime], Dict] = {}

    print("Sensor consumer with tumbling windows started...")

    try:
        while True:
            msg = consumer.poll(1.0)

            # periodisches Flushen abgeschlossener Fenster:
            now = datetime.now(timezone.utc)
            completed_rows: List[Tuple] = []
            to_delete = []

            for (sensor_id, w_start), state in windows.items():
                w_end = w_start + timedelta(seconds=WINDOW_SIZE_SECONDS)
                if now >= w_end:
                    # Fenster ist abgeschlossen -> Aggregation berechnen
                    n = state["count"]
                    if n == 0:
                        to_delete.append((sensor_id, w_start))
                        continue

                    avg_temp = state["sum_temp"] / n if state["sum_temp_count"] > 0 else None
                    avg_hum = state["sum_hum"] / n if state["sum_hum_count"] > 0 else None
                    avg_press = state["sum_press"] / n if state["sum_press_count"] > 0 else None

                    # Varianz/Std nur über Temperatur
                    if state["sum_temp_count"] > 1:
                        mean = avg_temp
                        var = (state["sum_temp_sq"] / state["sum_temp_count"]) - (mean ** 2)
                        std_temp = var ** 0.5 if var > 0 else 0.0
                    else:
                        std_temp = None

                    outlier_ratio = (
                        state["outlier_count"] / n if n > 0 else 0.0
                    )

                    completed_rows.append(
                        (
                            sensor_id,
                            w_start,
                            w_end,
                            n,
                            avg_temp,
                            std_temp,
                            state["min_temp"],
                            state["max_temp"],
                            avg_hum,
                            state["min_hum"],
                            state["max_hum"],
                            avg_press,
                            state["min_press"],
                            state["max_press"],
                            outlier_ratio,
                        )
                    )
                    to_delete.append((sensor_id, w_start))

            if completed_rows:
                execute_values(
                    cur,
                    """
                    INSERT INTO sensor_aggregates (
                        sensor_id,
                        window_start,
                        window_end,
                        count_readings,
                        avg_temperature,
                        std_temperature,
                        min_temperature,
                        max_temperature,
                        avg_humidity,
                        min_humidity,
                        max_humidity,
                        avg_pressure,
                        min_pressure,
                        max_pressure,
                        outlier_ratio
                    ) VALUES %s
                    """,
                    completed_rows,
                )
                conn.commit()
                consumer.commit()
                print(f"Inserted {len(completed_rows)} window aggregates")

            for key in to_delete:
                windows.pop(key, None)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Consumer error: {msg.error()}", file=sys.stderr)
                continue

            parsed = parse_message(msg.value())
            if parsed is None:
                continue

            sensor_id, ts, temp, hum, press = parsed
            w_start = floor_to_window(ts)
            key = (sensor_id, w_start)
            state = windows.get(key)
            if state is None:
                state = {
                    "count": 0,
                    "sum_temp": 0.0,
                    "sum_temp_sq": 0.0,
                    "sum_temp_count": 0,
                    "min_temp": None,
                    "max_temp": None,
                    "sum_hum": 0.0,
                    "sum_hum_count": 0,
                    "min_hum": None,
                    "max_hum": None,
                    "sum_press": 0.0,
                    "sum_press_count": 0,
                    "min_press": None,
                    "max_press": None,
                    "outlier_count": 0,
                }
                windows[key] = state

            state["count"] += 1

            if temp is not None:
                state["sum_temp"] += temp
                state["sum_temp_sq"] += temp * temp
                state["sum_temp_count"] += 1
                state["min_temp"] = temp if state["min_temp"] is None else min(state["min_temp"], temp)
                state["max_temp"] = temp if state["max_temp"] is None else max(state["max_temp"], temp)
                if is_outlier_temp(temp):
                    state["outlier_count"] += 1

            if hum is not None:
                state["sum_hum"] += hum
                state["sum_hum_count"] += 1
                state["min_hum"] = hum if state["min_hum"] is None else min(state["min_hum"], hum)
                state["max_hum"] = hum if state["max_hum"] is None else max(state["max_hum"], hum)

            if press is not None:
                state["sum_press"] += press
                state["sum_press_count"] += 1
                state["min_press"] = press if state["min_press"] is None else min(state["min_press"], press)
                state["max_press"] = press if state["max_press"] is None else max(state["max_press"], press)

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        cur.close()
        conn.close()
        consumer.close()


if __name__ == "__main__":
    main()

