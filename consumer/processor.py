import collections, json, kafka, time, os, redis
from pydoc_data.topics import topics

from dotenv import load_dotenv
from datetime import datetime
import pandas
import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaConsumer


# os even more nonsense just to read from a damn env file (the simple way wasn't working and this was the fix given by Claude)
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

required = ["TOPICS", "BOOTSTRAP_SERVERS", "AUTO_OFFSET_RESET", "CONSUMER_TIMEOUT_MS", "OUTPUT_DIR", "PORT"]
missing = [key for key in required if os.getenv(key) is None]
if missing:
    raise EnvironmentError(f"Missing required .env variables: {missing}")

# end of that nonsense

# retrieving needed kafka configs from .env file
FLUSH_TIMER = 10
TOPICS = os.getenv("TOPICS").split(", ")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET")
CONSUMER_TIMEOUT_MS = int(os.getenv("CONSUMER_TIMEOUT_MS"))
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./parquet_files")
VALUE_DESERIALIZER = lambda x: json.loads(x.decode('utf-8'))

#redis config
PORT = os.getenv("PORT")

os.makedirs(OUTPUT_DIR, exist_ok=True)

#kafka consumer instance init and config
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=[BOOTSTRAP_SERVERS],
    auto_offset_reset=AUTO_OFFSET_RESET,
    value_deserializer=VALUE_DESERIALIZER,
    consumer_timeout_ms=CONSUMER_TIMEOUT_MS
)
r = redis.Redis(host='localhost', port=PORT, decode_responses=True)


def flush_buffer(buffer):
    '''func that will handle transforming output from a buffer(array) to parquet file then clearing the buffer '''
    if not buffer:
        print("Buffer empty, nothing to flush.")
        return
    df = pandas.DataFrame(buffer)
    now = datetime.now()
    filename = os.path.join(OUTPUT_DIR, f"event_{now.strftime('%Y-%d-%m_%H-%M-%S')}.parquet")
    df.to_parquet(filename)
    print(f"Flushed {len(buffer)} records to {filename}")
    buffer.clear()

try:
    buffer = []
    start = time.perf_counter()

    while True:
        for message in consumer: #reading form kafka events
            if message is not None:
                print(f"Message received: {message.topic}")
                buffer.append(message.value)

                if message.topic == "impressions":
                    r.incr(f"campaign:{message.value["campaign_id"]}:impressions")
                if message.topic == "clicks":
                    r.incr(f"campaign:{message.value["campaign_id"]}:clicks")
                if message.topic == "bids":
                    r.incr(f"campaign:{message.value["campaign_id"]}:bids")
                    r.incrbyfloat(f"campaign:{message.value["campaign_id"]}:total_spend", message.value["bid_price"])

        elapsed = time.perf_counter() - start
        if elapsed >= FLUSH_TIMER:
            flush_buffer(buffer)
            start = time.perf_counter()

except KeyboardInterrupt:
    print("User interrupted — flushing remaining buffer...")
    flush_buffer(buffer)
finally:
    consumer.close()
    print("Consumer closed.")