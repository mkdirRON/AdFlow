import collections, json, kafka
from collections import defaultdict
from kafka import KafkaProducer

TOPICS = ["clicks", "impressions", "bids"]
BOOTSTRAP_SERVERS = ["localhost:9092"]
AUTO_OFFSET_RESET='earliest'

consumer = kafka.KafkaConsumer(*TOPICS,
                               bootstrap_servers=BOOTSTRAP_SERVERS,
                               auto_offset_reset=AUTO_OFFSET_RESET,
                               value_deserializer=lambda x: json.loads(x.decode('utf-8')))

impression_map = defaultdict(int)
click_map = defaultdict(int)

try:
    for message in consumer:

        if message is not None:
            print(f"Message received. Topic: {message.topic}")
            if message.topic == "impressions":
                impression_map[message.value["campaign_id"]] += 1

            if message.topic == "clicks":
                click_map[message.value["campaign_id"]] += 1

except KeyboardInterrupt as e:
    print(f'user interrupted. {e}')

print(f" number of impressions per campaign:"
      f" {impression_map}" + "\n")


CTR_map = defaultdict(float)
# loop through both impression dicts,
# count the number of impressions and clicks a simgle campaign id has
# return click/impressions for each campaign

for key in impression_map:
    if click_map[key] == 0:
        CTR_map[key] = 0
    else:
        CTR_map[key] = click_map[key]/impression_map[key]

print(f"CTR map: {CTR_map}")
