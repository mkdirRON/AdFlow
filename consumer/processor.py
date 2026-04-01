import collections, json, kafka, time, datetime
from datetime import datetime
import pandas
import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaProducer

# need to look into adding config into .env file
FLUSH_TIMER = 10
TOPICS = ["clicks", "impressions", "bids"]
BOOTSTRAP_SERVERS = ["localhost:9092"]
AUTO_OFFSET_RESET='earliest'
VALUE_DESERIALIZER =lambda x: json.loads(x.decode('utf-8'))

consumer = kafka.KafkaConsumer(*TOPICS,
                               bootstrap_servers=BOOTSTRAP_SERVERS,
                               auto_offset_reset=AUTO_OFFSET_RESET,
                               value_deserializer= VALUE_DESERIALIZER
                             )
 # add msgs to a buffer
 # after 10 seconds, flush buffer into parquet file
 # clear buffer, reset time, rinse and repeat.

try:
    buffer = [] # hold each batch of msgs into list

    start = time.perf_counter()
    for message in consumer:

        if message is not None:
            curr_timer = time.perf_counter() - start
            if float(curr_timer) < FLUSH_TIMER: # flush_timer is currently set to 10 seconds
                buffer.append(message.value)
            else: # 10 seconds have passed flush buffer to a parquet and reset time
                df = pandas.DataFrame(buffer)
                now = datetime.now()
                df.to_parquet(f"event_{now.strftime('%Y-%d-%m_%H-%M-%S_')}.parquet")
                buffer.clear()
                start = time.perf_counter()

except KeyboardInterrupt as e:
    print(f'user interrupted. {e}')
