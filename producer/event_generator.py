import json, random, uuid, time, argparse
from collections import defaultdict
from datetime import datetime

start_time = time.perf_counter()


def create_impression():
    #creating and return JSON imitating Ad bidding data
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "impression",
        "timestamp": str(datetime.now()),
        "user_id": f"user_{random.randint(1, 100000)}",
        "campaign_id": f"camp_id{random.randint(1, 100000)}",
        "bid_price": round(random.uniform(0.15, 0.5), 4),
        "site": random.choice(["news.com", "sports.com", "finance.com"])
    }

#adding rate of event generation. should be passed as an arg

parser = argparse.ArgumentParser()
parser.add_argument("--rate",
                    type=int,
                    default=1000,
                    help="the number of events that should be generated per second")
args = parser.parse_args()

#gerating events and storing
with open("../consumer/events.json", "w") as f:
    for _ in range(10000):
        iteration_start_time = time.perf_counter() # start timer to collect time elapsed for a single event
        f.write(json.dumps(create_impression()) + "\n")
        iteration_end_time = time.perf_counter() # ending timer

        itr_time = iteration_end_time - iteration_start_time # time it took for one event
        sleep_time = 1/args.rate - itr_time  # 1/[amount of events you want a second] - time it took for one event
        if sleep_time < 0:
            sleep_time = 0
        time.sleep(sleep_time)



end_time = time.perf_counter()
print(f"Time taken: {end_time - start_time} seconds")










