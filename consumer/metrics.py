from prometheus_client import Counter, Gauge

impression_counter = Counter('adflow_impressions', "number of recorded impressions")
bid_counter = Counter('adflow_bids', 'number of recorded bids')
click_counter = Counter('adflow_clicks', 'number of recorded clicks')

consumer_lag = Gauge('adflow_consumer_lag', 'consumer lag')