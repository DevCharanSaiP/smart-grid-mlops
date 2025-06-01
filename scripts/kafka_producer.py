import pandas as pd
from kafka import KafkaProducer
import json
import time
import os

# --- Configuration ---
DATA_FILE = os.path.join('data', 'smart_grid_dataset.csv')
KAFKA_BROKER = 'localhost:9092' # Kafka broker address
KAFKA_TOPIC = 'raw_energy_data' # Topic for raw data
# Simulate 15-minute data arriving every X seconds. Adjust for testing speed.
# For a real-world 15-min interval, you might want to simulate 15*60 = 900 seconds delay.
# For quicker testing, use a smaller value like 0.1 or 1 second.
STREAM_INTERVAL_SECONDS = 0.1 # Simulate rapid ingestion for testing

def load_and_prepare_data(file_path):
    """Loads the dataset, sorts by timestamp, and converts to appropriate types."""
    print(f"Loading data from {file_path}...")
    df = pd.read_csv(file_path, parse_dates=['Timestamp'])
    df = df.sort_values('Timestamp').reset_index(drop=True)
    print(f"Data loaded. Total records: {len(df)}")
    return df

def create_kafka_producer(broker):
    """Creates and returns a KafkaProducer instance."""
    print(f"Connecting to Kafka broker at {broker}...")
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(0, 11, 5) # Recommended for compatibility
    )
    print("Kafka Producer created.")
    return producer

def stream_data_to_kafka(df, producer, topic, interval):
    """Streams DataFrame rows as JSON messages to Kafka."""
    print(f"Starting to stream data to Kafka topic: {topic} with interval {interval}s")
    try:
        for index, row in df.iterrows():
            # Convert row to dictionary, handle NaNs if any (json.dumps handles them as null)
            message = row.where(pd.notnull(row), None).to_dict()
            producer.send(topic, value=message)
            print(f"Sent record {index+1}/{len(df)}: {message['Timestamp']}")
            time.sleep(interval)
    except Exception as e:
        print(f"An error occurred during streaming: {e}")
    finally:
        producer.flush() # Ensure all messages are sent
        print("Finished streaming data.")

if __name__ == "__main__":
    if not os.path.exists(DATA_FILE):
        print(f"Error: Dataset file not found at {DATA_FILE}.")
        print("Please download 'smart_grid_dataset.csv' from Kaggle and place it in the 'data/' directory.")
        exit(1)

    data_df = load_and_prepare_data(DATA_FILE)
    kafka_producer = create_kafka_producer(KAFKA_BROKER)
    stream_data_to_kafka(data_df, kafka_producer, KAFKA_TOPIC, STREAM_INTERVAL_SECONDS)