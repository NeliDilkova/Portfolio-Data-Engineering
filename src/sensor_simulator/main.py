'''
Generiert kontinuierlich Events wie:
sensor_id, timestamp, temperature, humidity, pressure.

'''
import random
import json
import time
from datetime import datetime, timezone
from confluent_kafka import Producer 

TOPIC = "sensor_readings_raw"
BOOTSTRAP_SERVERS = "localhost:9092"  # Kafka aus Docker

def generate_sensor_event(sensor_id: int) -> dict:
    base_temp = 20.0 + sensor_id
    base_humidity = 40.0 + sensor_id
    base_pressure = 1000.0

    temperature = random.gauss(base_temp, 1.5)
    humidity = random.uniform(base_humidity - 5, base_humidity + 5)
    pressure = random.gauss(base_pressure, 3.0)

    return {
        "sensor_id": sensor_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": round(temperature, 2),
        "humidity": round(humidity, 2),
        "pressure": round(pressure, 2),
    }

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    # sonst still

def run_simulator(num_sensors: int = 5, interval_seconds: float = 1.0) -> None:
    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "sensor-simulator",
    }
    producer = Producer(conf)

    print(
        f"Starting sensor simulator for {num_sensors} sensors, "
        f"interval={interval_seconds} seconds. Press Ctrl+C to stop."
    )

    try:
        while True:
            for sensor_id in range(1, num_sensors + 1):
                event = generate_sensor_event(sensor_id)
                producer.produce(
                    TOPIC,
                    key=str(sensor_id),
                    value=json.dumps(event).encode("utf-8"),
                    callback=delivery_report,
                )
                print(f"Sent to Kafka: {event}")
            producer.flush()
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        print("Sensor simulator stopped.")
    finally:
        producer.flush()

if __name__ == "__main__":
    run_simulator()

