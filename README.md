# AdFlow: The high-throughput data pipline 

# adFlow (Ad Data Pipeline)

adFlow is a real-time data pipeline designed to simulate, stream, and process advertising technology events.

Currently, the project focuses on the ingestion phase, utilizing an event generator to mock traffic and a Kafka-based consumer to batch process the streams into Parquet files for downstream analytics.

## Project Structure

![Screenshot 2026-04-02 at 1.24.45 PM.png](../../../../../var/folders/l_/wc62psx90fxdr1r8lzx1mpl40000gn/T/TemporaryItems/NSIRD_screencaptureui_I7tL5k/Screenshot%202026-04-02%20at%201.24.45%E2%80%AFPM.png)

## Current Components

### 1. Kafka Infrastructure (`compose.yaml`)
A Docker Compose setup that spins up an Apache Kafka broker (KRaft mode) and automatically initializes the required topics.
* **Topics created:** `impressions`, `bids`, `clicks`.

### 2. Event Generator (`producer/event_generator.py`)
A Python script that generates synthetic ad tech data.
* **Events:** Creates randomized impressions, clicks (with a simulated conversion flag), and bids (with density and pricing).
* **Data Points:** Each event includes a UUID, timestamp, user ID, campaign ID, and site domain.
* **Rate Limiting:** Supports a `--rate` argument to control how many events are generated per second.

### 3. Data Processor (`consumer/processor.py`)
A Python-based Kafka consumer that acts as a buffer.
* Listens to the configured Kafka topics.
* Buffers incoming JSON messages and flushes them to `.parquet` files using pandas and pyarrow every 10 seconds.

## Prerequisites
* [Docker](https://docs.docker.com/get-docker/) & Docker Compose
* Python 3.8+
![Screenshot 2026-04-02 at 1.24.45 PM.png](../../../../../var/folders/l_/wc62psx90fxdr1r8lzx1mpl40000gn/T/TemporaryItems/NSIRD_screencaptureui_I7tL5k/Screenshot%202026-04-02%20at%201.24.45%E2%80%AFPM.png)
## Setup & Execution

**1. Set up Environment Variables**
Create a `.env` file in the root directory (one level above your scripts) with the following variables:
\`\`\`env
TOPICS=impressions, bids, clicks
BOOTSTRAP_SERVERS=localhost:9092
AUTO_OFFSET_RESET=earliest
CONSUMER_TIMEOUT_MS=1000
OUTPUT_DIR=./parquet_files
\`\`\`

**2. Start the Kafka Broker**
Spin up the Kafka broker and initialize the topics:
\`\`\`bash
docker compose up -d
\`\`\`

**3. Install Python Dependencies**
\`\`\`bash
pip install -r requirements.txt
\`\`\`

**4. Run the Pipeline**
In one terminal, start the consumer to listen for messages:
\`\`\`bash
python consumer/processor.py
\`\`\`

In a second terminal, start the producer to begin generating traffic (defaults to 1000 events/second):
\`\`\`bash
python producer/event_generator.py --rate 1000
\`\`\`

## Future Work
* **Storage:** Integrate cloud storage (e.g., AWS S3, Google Cloud Storage) to upload the generated Parquet files.
* **Dashboard:** Build an analytics dashboard (e.g., Streamlit, Grafana, or Superset) to visualize bid prices, click-through rates, and impression volumes.