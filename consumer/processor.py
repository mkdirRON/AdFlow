import collections, json
from collections import defaultdict




impression_map = defaultdict(int)
click_map = defaultdict(int)
with open("events.json", "r") as f:
    for line in f:
        try:
            event = json.loads(line)
            if event["event_type"] == "impression":
                impression_map[event["campaign_id"]] += 1

            if event["event_type"] == "click":
                click_map[event["campaign_id"]] += 1
        except json.decoder.JSONDecodeError as e:

            print(f"error parsing event: {e}")

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


