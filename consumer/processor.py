import collections, json, kafka, time, datetime
from datetime import datetime


import pandas
import pyarrow as pa
import pyarrow.parquet as pq
from collections import defaultdict
from kafka import KafkaProducer


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

try:
    buffer = []
    start = time.perf_counter()
    for message in consumer:

        if message is not None:
            curr_timer = time.perf_counter() - start
            if float(curr_timer) < FLUSH_TIMER:
                buffer.append(message.value)
            else:
                df = pandas.DataFrame(buffer)
                now = datetime.now()
                df.to_parquet(f"event_{now.strftime('%Y-%d-%m_%H-%M-%S_')}.parquet")
                buffer.clear()
                start = time.perf_counter()



except KeyboardInterrupt as e:
    print(f'user interrupted. {e}')




# init a list that will store the events in batches. Once list is init, start timer.
# once timer has hit a certain point, end timer, flush the list into a parquet file.
# the parquet file should be named the end-timer time stamp.
#rinse and repeat.





# CTR_map = defaultdict(float)
# # loop through both impression dicts,
# # count the number of impressions and clicks a simgle campaign id has
# # return click/impressions for each campaign
# for key in impression_map:
#     if click_map[key] == 0:
#         CTR_map[key] = 0
#     else:
#         CTR_map[key] = click_map[key]/impression_map[key]
# print(f"CTR map: {CTR_map}")
