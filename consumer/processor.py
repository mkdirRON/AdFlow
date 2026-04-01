import collections, json, kafka, time
from datetime import datetime
import pandas
import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaConsumer

FLUSH_TIMER = 10
TOPICS = ["clicks", "impressions", "bids"]
BOOTSTRAP_SERVERS = ["localhost:9092"]
AUTO_OFFSET_RESET = 'earliest'
VALUE_DESERIALIZER = lambda x: json.loads(x.decode('utf-8'))

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset=AUTO_OFFSET_RESET,
    value_deserializer=VALUE_DESERIALIZER,
    consumer_timeout_ms=1000  # fixed this: unblocks the for-loop every 1s so flush can trigger even if no messages arrive
)

def flush_buffer(buffer):
    # Write buffer to a timestamped parquet file and clear it.
    if not buffer:
        print("Buffer empty, nothing to flush.")
        return
    df = pandas.DataFrame(buffer)
    now = datetime.now()
    filename = f"parquet_files/event_{now.strftime('%Y-%d-%m_%H-%M-%S')}.parquet"
    df.to_parquet(filename)
    print(f"Flushed {len(buffer)} records to {filename}")
    buffer.clear()

try:
    buffer = []
    start = time.perf_counter()

    while True:
        for message in consumer:
            if message is not None:
                print(f"Message received: {message.topic}")
                buffer.append(message.value)  # FIX: always append BEFORE checking timer

        elapsed = time.perf_counter() - start
        if elapsed >= FLUSH_TIMER:
            flush_buffer(buffer)
            start = time.perf_counter()

except KeyboardInterrupt:
    print("User interrupted — flushing remaining buffer...")
    flush_buffer(buffer)  # fixed this: flush whatever is left on exit
finally:
    consumer.close()
    print("Consumer closed.")