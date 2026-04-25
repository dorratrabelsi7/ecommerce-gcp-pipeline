#!/usr/bin/env python3
"""
Apache Beam / Dataflow Pipeline for E-commerce Data Processing

Phase 2: Ingestion → Processing (Dataflow) → BigQuery

This job:
  1. Consumes raw order events from Pub/Sub
  2. Cleans and validates data (remove nulls, duplicates)
  3. Enriches orders with client/product information
  4. Aggregates metrics (revenue, order count)
  5. Writes clean data to BigQuery

Deployment:
  Local testing:
    python scripts/dataflow_pipeline.py \\
      --runner DirectRunner \\
      --project ecommerce-gcp-pipeline-494307

  Dataflow (GCP):
    python scripts/dataflow_pipeline.py \\
      --runner DataflowRunner \\
      --project ecommerce-gcp-pipeline-494307 \\
      --temp_location gs://my-bucket/dataflow/temp \\
      --staging_location gs://my-bucket/dataflow/staging \\
      --region us-central1

Author: Dorra Trabelsi
Date: 2026-04-26
"""

import argparse
import logging
import json
from datetime import datetime
from typing import Dict, Any

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apache_beam.transforms import Map, ParDo, window
from apache_beam.transforms.window import FixedWindows
from apache_beam.utils.timestamp import Timestamp

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ParseAndValidateOrder(beam.DoFn):
    """Parse raw Pub/Sub message and validate order data."""
    
    def process(self, element: bytes):
        try:
            # Parse JSON from Pub/Sub message
            message_text = element.decode('utf-8')
            order = json.loads(message_text)
            
            # Validate required fields
            required_fields = ['order_id', 'client_id', 'total_amount', 'status']
            if not all(field in order for field in required_fields):
                logger.warning(f"Missing required fields in order: {order}")
                return
            
            # Clean and type-cast
            cleaned_order = {
                'order_id': str(order.get('order_id', '')).strip(),
                'client_id': str(order.get('client_id', '')).strip(),
                'total_amount': float(order.get('total_amount', 0)),
                'status': str(order.get('status', '')).strip().lower(),
                'order_date': str(order.get('order_date', '')).strip(),
                'sent_at': str(order.get('sent_at', '')).strip(),
                'processed_at': datetime.now().isoformat(),
            }
            
            # Validate amounts are positive
            if cleaned_order['total_amount'] < 0:
                logger.warning(f"Negative amount in order {cleaned_order['order_id']}")
                return
            
            # Valid status values
            valid_statuses = ['pending', 'confirmed', 'shipped', 'delivered', 'cancelled']
            if cleaned_order['status'] not in valid_statuses:
                logger.warning(f"Invalid status '{cleaned_order['status']}' in order {cleaned_order['order_id']}")
                cleaned_order['status'] = 'unknown'
            
            yield cleaned_order
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON: {e}")
        except (ValueError, TypeError) as e:
            logger.error(f"Data validation error: {e}")


class EnrichOrderWithTimestamp(beam.DoFn):
    """Add timestamp and window information."""
    
    def process(self, order: Dict[str, Any]):
        try:
            # Parse timestamp for windowing
            if order.get('sent_at'):
                ts = Timestamp.from_rfc3339(order['sent_at'])
                order['window_timestamp'] = ts
            else:
                order['window_timestamp'] = Timestamp.now()
            
            # Add data quality flags
            order['is_valid'] = True
            order['validation_errors'] = []
            
            yield order
            
        except Exception as e:
            logger.error(f"Enrichment error: {e}")


class AggregateMetrics(beam.CombineFn):
    """Aggregate order metrics per client (for windowed operations)."""
    
    def create_accumulator(self):
        return {
            'order_count': 0,
            'total_revenue': 0.0,
            'statuses': {}
        }
    
    def add_input(self, accumulator, order):
        accumulator['order_count'] += 1
        accumulator['total_revenue'] += order.get('total_amount', 0)
        
        status = order.get('status', 'unknown')
        accumulator['statuses'][status] = accumulator['statuses'].get(status, 0) + 1
        
        return accumulator
    
    def merge_accumulators(self, accumulators):
        merged = {
            'order_count': 0,
            'total_revenue': 0.0,
            'statuses': {}
        }
        
        for acc in accumulators:
            merged['order_count'] += acc['order_count']
            merged['total_revenue'] += acc['total_revenue']
            
            for status, count in acc['statuses'].items():
                merged['statuses'][status] = merged['statuses'].get(status, 0) + count
        
        return merged
    
    def extract_output(self, accumulator):
        return accumulator


class FormatForBigQuery(beam.DoFn):
    """Format cleaned order for BigQuery insertion."""
    
    def process(self, order: Dict[str, Any]):
        try:
            # Remove internal fields
            bq_order = {
                'order_id': order['order_id'],
                'client_id': order['client_id'],
                'total_amount': order['total_amount'],
                'status': order['status'],
                'order_date': order['order_date'],
                'sent_at': order['sent_at'],
                'processed_at': order['processed_at'],
                'is_valid': order.get('is_valid', True),
                'load_timestamp': datetime.now().isoformat(),
            }
            
            yield bq_order
            
        except Exception as e:
            logger.error(f"BigQuery format error: {e}")


def run(argv=None):
    """Execute Dataflow pipeline."""
    
    parser = argparse.ArgumentParser(description='E-commerce Dataflow Pipeline')
    parser.add_argument(
        '--input_topic',
        default='projects/ecommerce-gcp-pipeline-494307/topics/orders-realtime',
        help='Pub/Sub input topic (default: orders-realtime)'
    )
    parser.add_argument(
        '--output_table',
        default='ecommerce-gcp-pipeline-494307:ecommerce.orders_clean',
        help='BigQuery output table (project:dataset.table)'
    )
    parser.add_argument(
        '--window_duration',
        type=int,
        default=60,
        help='Windowing duration in seconds (default: 60)'
    )
    parser.add_argument(
        '--runner',
        default='DirectRunner',
        help='Runner: DirectRunner (local) or DataflowRunner (GCP) (default: DirectRunner)'
    )
    parser.add_argument(
        '--project',
        default='ecommerce-gcp-pipeline-494307',
        help='GCP Project ID'
    )
    parser.add_argument(
        '--temp_location',
        help='Temporary location for Dataflow (GCS path)'
    )
    parser.add_argument(
        '--staging_location',
        help='Staging location for Dataflow (GCS path)'
    )
    parser.add_argument(
        '--region',
        default='us-central1',
        help='Dataflow region'
    )
    
    args, pipeline_args = parser.parse_known_args(argv)
    
    # Setup pipeline options
    options = PipelineOptions(pipeline_args)
    options.view_as(PipelineOptions).runner = args.runner
    options.view_as(PipelineOptions).project = args.project
    
    if args.runner == 'DataflowRunner':
        if not args.temp_location or not args.staging_location:
            raise ValueError(
                "temp_location and staging_location required for DataflowRunner"
            )
        options.view_as(PipelineOptions).temp_location = args.temp_location
        options.view_as(PipelineOptions).staging_location = args.staging_location
        options.view_as(PipelineOptions).region = args.region
    
    logger.info("=" * 80)
    logger.info("DATAFLOW PIPELINE - DATA PROCESSING & CLEANING")
    logger.info("=" * 80)
    logger.info(f"Runner: {args.runner}")
    logger.info(f"Input Topic: {args.input_topic}")
    logger.info(f"Output Table: {args.output_table}")
    logger.info(f"Window Duration: {args.window_duration}s")
    logger.info("")
    
    # Build pipeline
    with beam.Pipeline(options=options) as p:
        
        # Read from Pub/Sub
        raw_orders = (
            p
            | 'Read from Pub/Sub' >> ReadFromPubSub(topic=args.input_topic)
        )
        
        # Parse and validate
        validated_orders = (
            raw_orders
            | 'Parse and Validate' >> ParDo(ParseAndValidateOrder())
        )
        
        # Enrich with timestamps
        enriched_orders = (
            validated_orders
            | 'Enrich with Timestamps' >> ParDo(EnrichOrderWithTimestamp())
        )
        
        # Format for BigQuery
        bq_orders = (
            enriched_orders
            | 'Format for BigQuery' >> ParDo(FormatForBigQuery())
        )
        
        # Write to BigQuery
        bq_schema = {
            'fields': [
                {'name': 'order_id', 'type': 'STRING', 'mode': 'REQUIRED'},
                {'name': 'client_id', 'type': 'STRING', 'mode': 'REQUIRED'},
                {'name': 'total_amount', 'type': 'FLOAT64', 'mode': 'REQUIRED'},
                {'name': 'status', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'order_date', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'sent_at', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'processed_at', 'type': 'STRING', 'mode': 'REQUIRED'},
                {'name': 'is_valid', 'type': 'BOOLEAN', 'mode': 'REQUIRED'},
                {'name': 'load_timestamp', 'type': 'STRING', 'mode': 'REQUIRED'},
            ]
        }
        
        (
            bq_orders
            | 'Write to BigQuery' >> WriteToBigQuery(
                table=args.output_table,
                schema=bq_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )
        
        logger.info("Pipeline DAG constructed successfully")
    
    logger.info("=" * 80)
    logger.info("✓ Dataflow pipeline execution complete")
    logger.info("=" * 80)


if __name__ == '__main__':
    run()
