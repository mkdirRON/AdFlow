import collections, json
from collections import defaultdict


impression_map = defaultdict(int)
with open("events.json", "r") as f:
    for line in f:
        try:
            event = json.loads(line)
            if event["event_type"] == "impression":
                impression_map[event["campaign_id"]] += 1
        except json.decoder.JSONDecodeError as e:

            print("error parsing event")

print(f" number of impressions per campaign:"
      f" {impression_map}")

