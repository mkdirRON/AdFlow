import json, random, uuid, time, argparse
from collections import defaultdict
from datetime import datetime

start_time = time.perf_counter()


def create_event():  # a base func that handles generic event creation

    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": str(datetime.now()),
        "user_id": f"user_{random.randint(1, 100000)}",
        "campaign_id": f"camp_id{random.randint(1, 100000)}",
        "site": random.choice(["news.com", "sports.com", "finance.com", "cars.com"])
    }

def create_impression(base_dict): # func that adds metadata that makes a genric event an "impression" should take a dict as input"
    impression = {"event_type": "impression"}
    return  base_dict | impression

def create_click(base_dict): # func that adds metadata that makes a genric event a "click" should take a dict as input
    click = {"event_type": "click",
             "conversion": random.choice([True, False])} #conversion rate is unrealistic. should try to simulate a rate of 1-3%
    return   base_dict | click

def create_bid(base_dict): # func that adds metadata that makes a genric event a "bid" should take a dict as input
    bid = {"event_type": "bid",
           "bid_density":random.randint(1, 6),
           "bid_price": round(random.uniform(0.15, 0.5), 4)
    }
    return base_dict | bid



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
        iteration_start_time = time.perf_counter()  # start timer to collect time elapsed for a single event

        f.write(json.dumps(random.choice(
            [create_impression(create_event()),
             create_bid(create_event()),
             create_click(create_event())]))
                + "\n")

        iteration_end_time = time.perf_counter() # ending timer

        itr_time = iteration_end_time - iteration_start_time # time it took for one event
        sleep_time = 1/args.rate - itr_time  # 1/[amount of events you want a second] - time it took for one event
        if sleep_time < 0:
            sleep_time = 0
        time.sleep(sleep_time)



end_time = time.perf_counter()
print(f"Time taken: {end_time - start_time} seconds")










