import psycopg2
from prometheus_client import start_http_server, Gauge
import time
import subprocess

METRIC_NAME_1 = 'can1_rate_157'
METRIC_NAME_2 = 'can2_rate_158'
METRIC_NAME_3 = 'cds_red_state_90'
METRIC_NAME_4 = 'canbus_speed' 
g1 = Gauge(METRIC_NAME_1, 'CAN1 Rate id=157')
g2 = Gauge(METRIC_NAME_2, 'CAN2 Rate id=158')
g3 = Gauge(METRIC_NAME_3, 'cds_red_state id=90')
g4 = Gauge(METRIC_NAME_4, 'CAN bus speed in kbps')

DB_CONFIG = {
    'dbname': '****',
    'user': '****',
    'password': '****',
    'host': 'localhost',
    'port': 5432
}

def get_canbus_speed():
    try:
        result = subprocess.run(
            ["ip", "-details", "-statistics", "-s", "link", "show", "can0"],
            capture_output=True,
            text=True,
            check=True
        )

        for line in result.stdout.splitlines():
            if "bitrate" in line:
                canbus_speed = int(line.split()[1]) / 1000
                return canbus_speed

        return None

    except Exception as e:
        print(f"Error while getting CAN bus speed: {e}")
        return None

def read_db_and_update_metrics(conn):
    try:
        cursor = conn.cursor()

        cursor.execute("SELECT value FROM fast_table WHERE id=157;")
        result_1 = cursor.fetchone()

        cursor.execute("SELECT value FROM fast_table WHERE id=158;")
        result_2 = cursor.fetchone()

        cursor.execute("SELECT value FROM fast_table WHERE id=90;")
        result_3 = cursor.fetchone()

        if result_1 is not None:
            g1.set(float(result_1[0]))
        else:
            print("No data returned for id=157")

        if result_2 is not None:
            g2.set(float(result_2[0]))
        else:
            print("No data returned for id=158")

        if result_3 is not None:
            g3.set(float(result_3[0]))
        else:
            print("No data returned for id=90")

        canbus_speed = get_canbus_speed()
        if canbus_speed is not None:
            g4.set(canbus_speed)

        cursor.close()

    except Exception as e:
        print(f"Error while reading from PostgreSQL: {e}")

if __name__ == '__main__':
    start_http_server(8000)

    try:
        conn = psycopg2.connect(**DB_CONFIG)

        while True:
            read_db_and_update_metrics(conn)
            time.sleep(15)

    except Exception as e:
        print(f"Error while connecting to PostgreSQL: {e}")

    finally:
        if conn:
            conn.close()
