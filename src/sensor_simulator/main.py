'''
Generiert kontinuierlich Events wie:
sensor_id, timestamp, temperature, humidity, pressure.

Noch kein Kafka, nur print, um Logik und Umgebung zu testen
'''
import random
import time
from datetime import datetime, timezone


def generate_sensor_event(sensor_id: int) -> dict:
    """Erzeugt ein einzelnes synthetisches Sensor-Event."""
    # Basiswerte pro Sensor leicht versetzt
    base_temp = 20.0 + sensor_id
    base_humidity = 40.0 + sensor_id
    base_pressure = 1000.0

    temperature = random.gauss(base_temp, 1.5)   # normalverteiltes Rauschen
    humidity = random.uniform(base_humidity - 5, base_humidity + 5) # gleichverteiltes Rauschen
    pressure = random.gauss(base_pressure, 3.0) # normalverteiltes Rauschen

    event = {
        "sensor_id": sensor_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": round(temperature, 2),
        "humidity": round(humidity, 2),
        "pressure": round(pressure, 2),
    }
    return event


def run_simulator(num_sensors: int = 5, interval_seconds: float = 1.0) -> None:
    """Lässt mehrere Sensoren periodisch Events erzeugen und auf die Konsole schreiben."""
    print(f"Starting sensor simulator for {num_sensors} sensors, "
          f"interval={interval_seconds} seconds. Press Ctrl+C to stop.")
    try:
        while True:
            for sensor_id in range(1, num_sensors + 1):
                event = generate_sensor_event(sensor_id)
                print(event)
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        print("Sensor simulator stopped.")


if __name__ == "__main__":
    run_simulator()
