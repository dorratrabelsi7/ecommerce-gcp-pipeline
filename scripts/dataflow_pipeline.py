#!/usr/bin/env python3
"""
Apache Beam / Dataflow Pipeline for E-commerce Data Processing

Phase 2: Ingestion → Processing (Dataflow) → BigQuery

Steps:
  1. Read raw orders from Pub/Sub
  2. Parse, clean, validate
  3. Enrich with clients/products (batch from GCS)
  4. Aggregate metrics (revenue, order count per client)
  5. Write clean data + metrics to BigQuery

Author: Dorra Trabelsi
Date: 2026-04-26
"""

import argparse
import logging
import json
import csv
from datetime import datetime
from typing import Dict, Any

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromPubSub, WriteToBigQuery

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ParseAndValidateOrder(beam.DoFn):
    """Parse raw Pub/Sub message and validate order data."""

    def process(self, element: bytes):
        try:
            order = json.loads(element.decode("utf-8"))
            required_fields = ["order_id", "client_id", "product_id", "total_amount", "status"]
            if not all(field in order for field in required_fields):
                return

            cleaned_order = {
                "order_id": str(order.get("order_id", "")).strip(),
                "client_id": str(order.get("client_id", "")).strip(),
                "product_id": str(order.get("product_id", "")).strip(),
                "total_amount": float(order.get("total_amount", 0)),
                "status": str(order.get("status", "")).strip().lower(),
                "order_date": order.get("order_date"),
                "sent_at": order.get("sent_at"),
                "processed_at": datetime.utcnow().isoformat(),
            }

            if cleaned_order["total_amount"] < 0:
                return

            valid_statuses = ["pending", "confirmed", "shipped", "delivered", "cancelled"]
            if cleaned_order["status"] not in valid_statuses:
                cleaned_order["status"] = "unknown"

            yield cleaned_order

        except Exception as e:
            logger.error(f"Parse error: {e}")


def parse_csv(line, expected_fields):
    """Robust CSV parser using csv.reader."""
    reader = csv.reader([line])
    fields = next(reader)
    if len(fields) < len(expected_fields):
        return None
    return dict(zip(expected_fields, fields))


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_topic", default="projects/ecommerce-gcp-pipeline-494307/topics/orders-realtime")
    parser.add_argument("--clients_file", default="gs://ecommerce-gcp-pipeline-494307-data/data/raw/clients.csv")
    parser.add_argument("--products_file", default="gs://ecommerce-gcp-pipeline-494307-data/data/raw/products.csv")
    parser.add_argument("--output_orders", default="ecommerce-gcp-pipeline-494307:ecommerce_analytics.cleaned_orders")
    parser.add_argument("--output_metrics", default="ecommerce-gcp-pipeline-494307:ecommerce_analytics.client_metrics")
    parser.add_argument("--runner", default="DataflowRunner")
    parser.add_argument("--project", default="ecommerce-gcp-pipeline-494307")
    parser.add_argument("--region", default="europe-west1")
    parser.add_argument("--temp_location", default="gs://ecommerce-gcp-pipeline-494307-data/temp")
    parser.add_argument("--staging_location", default="gs://ecommerce-gcp-pipeline-494307-data/data/staging")
    args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    options.view_as(PipelineOptions).runner = args.runner
    options.view_as(PipelineOptions).project = args.project
    options.view_as(PipelineOptions).region = args.region
    options.view_as(PipelineOptions).temp_location = args.temp_location
    options.view_as(PipelineOptions).staging_location = args.staging_location

    with beam.Pipeline(options=options) as p:
        # 🔹 Read orders from Pub/Sub
        orders = (
            p
            | "Read Orders PubSub" >> ReadFromPubSub(topic=args.input_topic)
            | "Parse Orders" >> beam.ParDo(ParseAndValidateOrder())
        )

        # 🔹 Read clients from GCS
        clients = (
            p
            | "Read Clients CSV" >> beam.io.ReadFromText(args.clients_file, skip_header_lines=1)
            | "Parse Clients" >> beam.Map(parse_csv, expected_fields=["client_id", "name", "email"])
            | "KV Clients" >> beam.Map(lambda c: (c["client_id"], c))
        )

        # 🔹 Read products from GCS
        products = (
            p
            | "Read Products CSV" >> beam.io.ReadFromText(args.products_file, skip_header_lines=1)
            | "Parse Products" >> beam.Map(parse_csv, expected_fields=["product_id", "name", "category"])
            | "KV Products" >> beam.Map(lambda p_: (p_["product_id"], p_))
        )

        # 🔹 Key orders by client_id and product_id
        orders_kv_client = orders | "KV Orders by Client" >> beam.Map(lambda o: (o["client_id"], o))
        orders_kv_product = orders | "KV Orders by Product" >> beam.Map(lambda o: (o["product_id"], o))

        # 🔹 Join orders with clients
        joined_client = (
            {"orders": orders_kv_client, "clients": clients}
            | "Join Orders Clients" >> beam.CoGroupByKey()
            | "Merge Client Data" >> beam.FlatMap(lambda kv: [
                {**order, **kv[1]["clients"][0]} for order in kv[1]["orders"] if kv[1]["clients"]
            ])
        )

        # 🔹 Join orders with products
        joined_product = (
            {"orders": orders_kv_product, "products": products}
            | "Join Orders Products" >> beam.CoGroupByKey()
            | "Merge Product Data" >> beam.FlatMap(lambda kv: [
                {**order, **kv[1]["products"][0]} for order in kv[1]["orders"] if kv[1]["products"]
            ])
        )

        # 🔹 Merge enriched data
        enriched_orders = ((joined_client, joined_product) | "Flatten Enriched Orders" >> beam.Flatten())

        # 🔹 Write cleaned orders to BigQuery
        enriched_orders | "Write Orders BQ" >> WriteToBigQuery(
            table=args.output_orders,
            schema="order_id:STRING, client_id:STRING, product_id:STRING, total_amount:FLOAT64, status:STRING, order_date:TIMESTAMP, sent_at:TIMESTAMP, processed_at:TIMESTAMP, name:STRING, email:STRING, category:STRING",
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )

        # 🔹 Aggregate metrics per client
        metrics = (
            enriched_orders
            | "KV Client Metrics" >> beam.Map(lambda o: (o["client_id"], o))
            | "Aggregate Metrics" >> beam.CombinePerKey(
                lambda orders: {
                    "order_count": len(orders),
                    "total_revenue": sum(o["total_amount"] for o in orders),
                }
            )
            | "Format Metrics" >> beam.Map(lambda kv: {
                "client_id": kv[0],
                "order_count": kv[1]["order_count"],
                "total_revenue": kv[1]["total_revenue"],
                "load_timestamp": datetime.utcnow().isoformat(),
            })
        )

        # 🔹 Write metrics to BigQuery
        metrics | "Write Metrics BQ" >> WriteToBigQuery(
            table=args.output_metrics,
            schema="client_id:STRING, order_count:INTEGER, total_revenue:FLOAT64, load_timestamp:TIMESTAMP",
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )

    logger.info("✓ Dataflow pipeline executed successfully")


if __name__ == "__main__":
    run()
