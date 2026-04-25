"""
Real-time stream simulator for e-commerce orders.

Simulates raw orders being published to Pub/Sub topic in real-time.
Dataflow (Apache Beam) will consume these raw events, clean them,
and load them into BigQuery.

Author: Dorra Trabelsi
Date: 2026-04-21
"""

import sys
import argparse
import json
import logging
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from google.cloud import pubsub_v1
from dotenv import load_dotenv
import os

# Load environment
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = os.getenv("PROJECT_ID", "ecommerce-gcp-pipeline-494307")
PUBSUB_TOPIC = os.getenv("PUBSUB_TOPIC", "orders-realtime")
TOPIC_PATH = f"projects/{PROJECT_ID}/topics/{PUBSUB_TOPIC}"
DATA_FILE = Path("data/raw/orders.csv")   # ⚠️ lire les données brutes

def main():
    """Publish raw order events to Pub/Sub."""
    parser = argparse.ArgumentParser(
        description="Simulate real-time raw order stream to Pub/Sub"
    )
    parser.add_argument("--limit", type=int, default=200,
                        help="Max messages to send (default: 200)")
    parser.add_argument("--speed", type=float, default=2.0,
                        help="Delay between messages in seconds (default: 2.0)")
    parser.add_argument("--verbose", action="store_true",
                        help="Print full message payload")
    args = parser.parse_args()

    # Validate data file
    if not DATA_FILE.exists():
        logger.error(f"Data file not found: {DATA_FILE}")
        logger.info("Upload raw orders.csv into data/raw/ first")
        return 1

    try:
        logger.info("=" * 80)
        logger.info("REAL-TIME RAW ORDER STREAM SIMULATOR")
        logger.info("=" * 80)
        logger.info(f"Pub/Sub Topic: {PUBSUB_TOPIC}")
        logger.info(f"Limit: {args.limit} messages")
        logger.info(f"Speed: {args.speed}s per message")
        logger.info("")

        # Load raw orders
        orders_df = pd.read_csv(DATA_FILE)
        logger.info(f"Loaded {len(orders_df):,} raw orders from {DATA_FILE}")

        # Initialize publisher
        publisher = pubsub_v1.PublisherClient()

        sent_count = 0
        error_count = 0
        start_time = time.time()

        logger.info("Starting publication...\n")

        # Publish messages
        for idx, row in orders_df.head(args.limit).iterrows():
            try:
                # Build raw message payload
                message = {
                    "order_id": row["order_id"],
                    "client_id": row["client_id"],
                    "total_amount": float(row["total_amount"]),
                    "status": row["status"],
                    "order_date": row["order_date"],   # brut, Dataflow va parser
                    "sent_at": datetime.now().isoformat(),
                }

                message_json = json.dumps(message)
                message_bytes = message_json.encode("utf-8")

                future = publisher.publish(TOPIC_PATH, message_bytes)
                future.result(timeout=10)

                sent_count += 1
                timestamp = datetime.now().strftime("%H:%M:%S")

                if args.verbose:
                    logger.info(f"[{timestamp}] Published: {message_json}")
                else:
                    if sent_count % 10 == 0 or sent_count == 1:
                        logger.info(
                            f"[{timestamp}] ✓ {row['order_id']} → "
                            f"{row['client_id']} → €{row['total_amount']:.2f} "
                            f"· {row['status']}"
                        )

                if idx < len(orders_df.head(args.limit)) - 1:
                    time.sleep(args.speed)

            except Exception as e:
                logger.error(f"✗ Failed to publish message: {str(e)}")
                error_count += 1

        elapsed = time.time() - start_time
        msg_per_sec = sent_count / elapsed if elapsed > 0 else 0

        logger.info("")
        logger.info("=" * 80)
        logger.info("STREAM SIMULATION COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Messages sent:  {sent_count}")
        logger.info(f"Errors:         {error_count}")
        logger.info(f"Duration:       {elapsed:.1f}s")
        logger.info(f"Rate:           {msg_per_sec:.2f} msg/s")
        logger.info("=" * 80)

        return 0 if error_count == 0 else 1

    except KeyboardInterrupt:
        logger.info("\n[INTERRUPTED] Stream simulation stopped by user")
        logger.info(f"Sent {sent_count} messages before interruption")
        return 0

    except Exception as e:
        logger.error(f"✗ Stream simulation failed: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())
