import json
import os
import urllib.request
from kafka import KafkaProducer
from datetime import datetime, timezone

def fetch_crypto_prices():
    url = (
        "https://api.coingecko.com/api/v3/coins/markets"
        "?vs_currency=usd"
        "&ids=bitcoin,ethereum,solana,binancecoin,ripple,cardano,dogecoin,polkadot,avalanche-2,chainlink,uniswap,litecoin,cosmos,stellar,monero"
        "&order=market_cap_desc"
        "&sparkline=false" #won't need graph data
        "&price_change_percentage=24h"
    )
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0"}
    )
    with urllib.request.urlopen(req, timeout=10) as response:
        return json.loads(response.read().decode())

def get_kafka_producer(bootstrap_servers):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
        request_timeout_ms=30000,
        api_version=(3, 9, 0),
    )

def lambda_handler(event, context):
    print(f"[{datetime.now(timezone.utc).isoformat()}] Lambda triggered")

    bootstrap_servers = os.environ["MSK_BOOTSTRAP_SERVERS"]

    # Function 1 - already tested ✅
    print("Fetching crypto prices...")
    records = fetch_crypto_prices()
    print(f"Fetched {len(records)} coins")

    # Function 2 - testing now
    print("Connecting to Kafka...") 
    producer = get_kafka_producer(bootstrap_servers)

    for record in records:
        producer.send(
            topic="crypto_prices_raw",
            key=record["id"],
            value=record,
        )
        print(f"Published: {record['id']} @ ${record['current_price']}")

    producer.flush()
    producer.close()
    print("All records published to Kafka")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "coins_fetched": len(records),
            "kafka_published": len(records),
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
    }