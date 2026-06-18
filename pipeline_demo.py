import os
import json
import time
import psycopg2
from kafka import KafkaProducer, KafkaConsumer

# ---------------------------------------------------------
# CONFIGURATION & ENVIRONMENT VARIABLES
# ---------------------------------------------------------
# The os.getenv("VARIABLE", "DEFAULT") syntax allows the script to read
# from system environment variables in production, while falling back
# to local dummy credentials for an effortless laptop demo.
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "iot_telemetry")

DB_PARAMS = {
    "dbname": os.getenv("DB_NAME", "analytics_dw"),
    "user": os.getenv("DB_USER", "datastreamx_user"),
    "password": os.getenv("DB_PASSWORD", "password123"),
    "host": os.getenv("DB_HOST", "localhost")
}

# ---------------------------------------------------------
# DATABASE INITIALIZATION (Setting up our Warehouse Table)
# ---------------------------------------------------------
def init_warehouse():
    print("[1/4] Initializing Local Data Warehouse Tables...")
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        # Create a table to store processed analytical data
        cur.execute("""
            CREATE TABLE IF NOT EXISTS device_analytics (
                id SERIAL PRIMARY KEY,
                device_id VARCHAR(50),
                temp_celsius FLOAT,
                temp_fahrenheit FLOAT,
                status VARCHAR(20),
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        cur.close()
        conn.close()
        print("      -> Warehouse tables ready.")
    except Exception as e:
        print(f"      [ERROR] Could not connect to database: {e}")
        print("      Ensure your Docker containers are running using 'docker-compose up -d'")
        exit(1)

# ---------------------------------------------------------
# PIPELINE STEP 1: Ingestion (The Producer)
# ---------------------------------------------------------
def run_mock_ingestion():
    print("\n[2/4] Starting Data Ingestion Stream...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_SERVER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Simulate a batch of 5 IoT device transmissions
        for i in range(1, 6):
            payload = {
                "device_id": f"DEV_00{i}",
                "temperature": 20.0 + (i * 1.5)  # Generates varied temps: 21.5, 23.0, 24.5, 26.0, 27.5
            }
            producer.send(TOPIC_NAME, value=payload)
            print(f"      [Ingested] Sent to Kafka: {payload}")
            time.sleep(0.5) # Simulate slight real-time stream delay
            
        producer.flush()
        print("      -> Ingestion batch complete.")
    except Exception as e:
        print(f"      [ERROR] Kafka Ingestion failed: {e}")
        exit(1)

# ---------------------------------------------------------
# PIPELINE STEP 2 & 3: Transformation & Storage (The Consumer)
# ---------------------------------------------------------
def run_stream_processor():
    print("\n[3/4] Launching Real-Time Stream Processor...")
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=[KAFKA_SERVER],
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=3000 # Stop processing if idle for 3 seconds
        )
        
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        
        for message in consumer:
            raw_data = message.value
            temp_c = raw_data['temperature']
            
            # --- TRANSFORMATION & ENRICHMENT LOGIC ---
            # 1. Unit Conversion
            temp_f = (temp_c * 9/5) + 32  
            # 2. Rule-based Alert Tagging
            status = "CRITICAL_HOT" if temp_c > 25.0 else "NORMAL"  
            
            # --- STORAGE LOGIC (Writing to Warehouse) ---
            cur.execute("""
                INSERT INTO device_analytics (device_id, temp_celsius, temp_fahrenheit, status)
                VALUES (%s, %s, %s, %s);
            """, (raw_data['device_id'], temp_c, temp_f, status))
            
            print(f"      [Processed & Saved] {raw_data['device_id']} -> {temp_f:.2f}°F ({status})")
        
        conn.commit()
        cur.close()
        conn.close()
        print("      -> Stream processing step completed successfully.")
    except Exception as e:
        print(f"      [ERROR] Stream processing failed: {e}")

# ---------------------------------------------------------
# VERIFICATION: Querying the Database for Results
# ---------------------------------------------------------
def verify_warehouse_data():
    print("\n[4/4] Querying Data Warehouse to Verify Output...")
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        
        cur.execute("SELECT id, device_id, temp_celsius, temp_fahrenheit, status FROM device_analytics;")
        rows = cur.fetchall()
        
        print("\n=== CURRENT WAREHOUSE STATE ===")
        print(f"{'ID':<5} | {'Device ID':<12} | {'Temp (C)':<10} | {'Temp (F)':<10} | {'Status':<12}")
        print("-" * 60)
        for row in rows:
            print(f"{row[0]:<5} | {row[1]:<12} | {row[2]:<10.1f} | {row[3]:<10.2f} | {row[4]:<12}")
            
        cur.close()
        conn.close()
    except Exception as e:
        print(f"      [ERROR] Verification query failed: {e}")

# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------
if __name__ == "__main__":
    init_warehouse()
    run_mock_ingestion()
    run_stream_processor()
    verify_warehouse_data()