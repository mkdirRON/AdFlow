
import redis, os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

PORT = int(os.getenv("PORT"))
r = redis.Redis(host='localhost', port=PORT, decode_responses=True)


campaign_map = {}

for key in r.scan_iter("campaign:*:*"):
    key = key.split(":")
    campaign_id = key[1]

    campaign_map[campaign_id] = {
        "impressions": int(r.get(f"campaign:{campaign_id}:impressions") or 0),
        "bids": int(r.get(f"campaign:{campaign_id}:bids") or 0),
        "clicks": int(r.get(f"campaign:{campaign_id}:clicks") or 0),
        "total_spent": float(r.get(f"campaign:{campaign_id}:total_spend"))
    }


print(campaign_map)


    # topic = key[2]
    # if topic == "impressions":
    #     value = int(r.get(f"campaign:{campaign_id}:impressions"))
    #     print(f"we gotta impression: {value}")
    # if topic == "bids":
    #     value = int(r.get(f"campaign:{campaign_id}:bids"))
    #     total_spent = float(r.get(f"campaign:{campaign_id}:total_spend"))
    #     print(f"we gotta bid: {value} and total spent: {total_spent}")
    #
    # if topic == "clicks":
    #     value = int(r.get(f"campaign:{campaign_id}:clicks"))
    #     print(f"we gotta click: {value}")



