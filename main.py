#!/usr/bin/env python3
"""
E-commerce GCP Pipeline Orchestrator

Main entry point for the real-time e-commerce data pipeline.
Orchestrates:
  1. Data generation (Faker)
  2. Real-time stream simulation (Pub/Sub)
  3. Data processing (Dataflow)
  4. BigQuery loading & analysis

Author: Dorra Trabelsi
Date: 2026-04-21
"""

import sys
import subprocess
import logging
import os
import click
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SCRIPTS_DIR = Path(__file__).parent / "scripts"
DATA_DIR = Path(__file__).parent / "data"


@click.group()
def cli():
    """E-commerce GCP Pipeline - Orchestrate data flow from ingestion to analytics."""
    pass


@cli.command()
@click.option('--rows', default=1000, help='Number of rows to generate (default: 1000)')
@click.option('--output-dir', default='data/raw', help='Output directory for generated data')
def generate(rows, output_dir):
    """
    Generate synthetic e-commerce data (Phase 1: Data Generation)
    
    Creates:
      - clients.csv
      - orders.csv
      - products.csv
      - incidents.csv
      - page_views.csv
    """
    logger.info("=" * 80)
    logger.info("PHASE 1: DATA GENERATION")
    logger.info("=" * 80)
    
    script = SCRIPTS_DIR / "generate_data.py"
    if not script.exists():
        logger.error(f"Script not found: {script}")
        return 1
    
    cmd = [
        sys.executable,
        str(script),
        "--rows", str(rows),
        "--output-dir", output_dir
    ]
    
    try:
        result = subprocess.run(cmd, check=True)
        logger.info("✓ Data generation complete")
        return result.returncode
    except subprocess.CalledProcessError as e:
        logger.error(f"✗ Data generation failed: {e}")
        return 1


@cli.command()
@click.option('--limit', default=200, help='Number of messages to stream (default: 200)')
@click.option('--speed', default=2.0, help='Delay between messages in seconds (default: 2.0)')
@click.option('--verbose', is_flag=True, help='Print full message payloads')
def stream(limit, speed, verbose):
    """
    Simulate real-time order stream to Pub/Sub (Phase 1: Ingestion)
    
    Requirements:
      - GCP authentication configured (gcloud auth application-default login)
      - Pub/Sub topic created in GCP
      - Environment variables set: PROJECT_ID, PUBSUB_TOPIC
    
    COST NOTE: Pub/Sub free tier = 10GB/month
      - Each message ~500 bytes
      - 200 messages = ~100KB (negligible cost)
      - Default --limit 200 is safe
    """
    logger.info("=" * 80)
    logger.info("PHASE 1: REAL-TIME STREAM SIMULATION (Pub/Sub)")
    logger.info("=" * 80)
    
    script = SCRIPTS_DIR / "simulate_realtime.py"
    if not script.exists():
        logger.error(f"Script not found: {script}")
        return 1
    
    cmd = [
        sys.executable,
        str(script),
        "--limit", str(limit),
        "--speed", str(speed)
    ]
    
    if verbose:
        cmd.append("--verbose")
    
    try:
        result = subprocess.run(cmd, check=True)
        logger.info("✓ Stream simulation complete")
        return result.returncode
    except subprocess.CalledProcessError as e:
        logger.error(f"✗ Stream simulation failed: {e}")
        return 1


@cli.command()
@click.option('--bucket', required=True, help='GCS bucket name (e.g., gs://my-bucket)')
@click.option('--credentials', help='Path to GCP service account key (optional)')
def upload(bucket, credentials):
    """
    Upload raw data to Cloud Storage (Phase 1: Ingestion)
    
    Uploads:
      - data/raw/*.csv to GCS bucket
    
    Requirements:
      - GCP authentication configured
      - Cloud Storage bucket already created
    """
    logger.info("=" * 80)
    logger.info("PHASE 1: UPLOAD TO CLOUD STORAGE")
    logger.info("=" * 80)
    
    script = SCRIPTS_DIR / "upload_to_gcs.py"
    if not script.exists():
        logger.error(f"Script not found: {script}")
        return 1
    
    cmd = [
        sys.executable,
        str(script),
        "--bucket", bucket
    ]
    
    if credentials:
        cmd.extend(["--credentials", credentials])
    
    try:
        result = subprocess.run(cmd, check=True)
        logger.info("✓ Upload to Cloud Storage complete")
        return result.returncode
    except subprocess.CalledProcessError as e:
        logger.error(f"✗ Upload failed: {e}")
        return 1


@cli.command()
@click.option('--runner', default='DirectRunner', 
              help='Beam runner: DirectRunner (local) or DataflowRunner (GCP) (default: DirectRunner)')
@click.option('--temp-location', help='GCS temp location for Dataflow (required for DataflowRunner)')
@click.option('--staging-location', help='GCS staging location for Dataflow (required for DataflowRunner)')
@click.option('--window-duration', default=60, help='Window duration in seconds (default: 60)')
def dataflow(runner, temp_location, staging_location, window_duration):
    """
    Deploy Dataflow pipeline for stream processing (Phase 2: Processing)
    
    Processes raw orders from Pub/Sub:
      - Validates & cleans data
      - Enriches with timestamps
      - Writes to BigQuery
    
    Requirements:
      - GCP authentication configured
      - Pub/Sub topic populated with messages
      - BigQuery dataset created
      - (For DataflowRunner) GCS bucket for temp/staging
    """
    logger.info("=" * 80)
    logger.info("PHASE 2: DATAFLOW PIPELINE (Stream Processing)")
    logger.info("=" * 80)
    
    script = SCRIPTS_DIR / "dataflow_pipeline.py"
    if not script.exists():
        logger.error(f"Script not found: {script}")
        return 1
    
    cmd = [
        sys.executable,
        str(script),
        "--runner", runner,
        "--window_duration", str(window_duration)
    ]
    
    if runner == "DataflowRunner":
        if not temp_location or not staging_location:
            logger.error("temp_location and staging_location required for DataflowRunner")
            logger.info("Usage: python main.py dataflow --runner DataflowRunner \\")
            logger.info("  --temp-location gs://my-bucket/temp \\")
            logger.info("  --staging-location gs://my-bucket/staging")
            return 1
        cmd.extend(["--temp_location", temp_location])
        cmd.extend(["--staging_location", staging_location])
    
    try:
        result = subprocess.run(cmd, check=True)
        logger.info("✓ Dataflow pipeline deployment complete")
        logger.info("\nNext steps:")
        if runner == "DataflowRunner":
            logger.info("  1. Monitor job in GCP Console: Cloud Dataflow")
            logger.info("  2. Check BigQuery table for incoming data")
        else:
            logger.info("  1. Messages are being processed locally")
            logger.info("  2. Check BigQuery for results")
        return result.returncode
    except subprocess.CalledProcessError as e:
        logger.error(f"✗ Dataflow deployment failed: {e}")
        return 1


@cli.command()
def pipeline():
    """
    Run complete pipeline: Generate → Stream → Dataflow
    
    Executes all phases:
      1. Generate synthetic data
      2. Simulate real-time stream to Pub/Sub
      3. Deploy Dataflow job for processing
    """
    logger.info("=" * 80)
    logger.info("ECOMMERCE GCP PIPELINE - FULL EXECUTION")
    logger.info("=" * 80)
    
    # Phase 1a: Generate data
    logger.info("\n[1/3] Generating synthetic data...")
    result = generate.callback(rows=1000, output_dir='data/raw')
    if result != 0:
        logger.error("Pipeline aborted: Data generation failed")
        return 1
    
    # Phase 1b: Stream to Pub/Sub
    logger.info("\n[2/3] Starting real-time stream to Pub/Sub...")
    logger.info("Make sure:")
    logger.info("  1. GCP authentication is configured: gcloud auth application-default login")
    logger.info("  2. Pub/Sub topic exists: projects/{PROJECT_ID}/topics/{PUBSUB_TOPIC}")
    logger.info("  3. Environment variables are set: PROJECT_ID, PUBSUB_TOPIC")
    
    confirm = click.confirm("Continue to Pub/Sub streaming?", default=True)
    if not confirm:
        logger.info("Pipeline cancelled")
        return 0
    
    result = stream.callback(limit=200, speed=2.0, verbose=False)
    if result != 0:
        logger.error("Pipeline aborted: Stream simulation failed")
        return 1
    
    # Phase 2: Dataflow
    logger.info("\n[3/3] Deploying Dataflow job for processing...")
    logger.info("Using DirectRunner (local processing). For production, use DataflowRunner")
    
    confirm = click.confirm("Deploy Dataflow job (local)?", default=True)
    if not confirm:
        logger.info("Dataflow skipped")
        logger.info("You can run it manually:")
        logger.info("  python main.py dataflow --runner DirectRunner")
        return 0
    
    result = dataflow.callback(runner='DirectRunner', temp_location=None, 
                               staging_location=None, window_duration=60)
    if result != 0:
        logger.error("Pipeline aborted: Dataflow deployment failed")
        return 1
    
    logger.info("\n" + "=" * 80)
    logger.info("✓ PIPELINE COMPLETE - All phases successful!")
    logger.info("=" * 80)
    logger.info("\nNext steps:")
    logger.info("  1. Verify BigQuery tables are populated")
    logger.info("  2. Run: python main.py status")
    logger.info("  3. Create dashboard in Looker Studio")
    logger.info("  4. Generate BI report from SQL queries")
    
    return 0


@cli.command()
def status():
    """
    Check pipeline status and GCP configuration
    
    Validates:
      - GCP authentication
      - Required files
      - Environment variables
    """
    logger.info("=" * 80)
    logger.info("PIPELINE STATUS CHECK")
    logger.info("=" * 80)
    
    checks_passed = 0
    checks_total = 0
    
    # Check GCP authentication
    checks_total += 1
    try:
        result = subprocess.run(
            ["gcloud", "auth", "list"],
            capture_output=True,
            timeout=5
        )
        if result.returncode == 0:
            logger.info("✓ GCP authentication configured")
            checks_passed += 1
        else:
            logger.warning("✗ GCP authentication not configured")
    except Exception as e:
        logger.warning(f"✗ GCP authentication check failed: {e}")
    
    # Check data directory
    checks_total += 1
    if DATA_DIR.exists():
        logger.info(f"✓ Data directory exists: {DATA_DIR}")
        checks_passed += 1
    else:
        logger.warning(f"✗ Data directory missing: {DATA_DIR}")
    
    # Check environment variables
    checks_total += 1
    required_vars = ["PROJECT_ID", "PUBSUB_TOPIC"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if not missing_vars:
        logger.info(f"✓ All required env vars set: {', '.join(required_vars)}")
        checks_passed += 1
    else:
        logger.warning(f"✗ Missing env vars: {', '.join(missing_vars)}")
        logger.info(f"   Create .env file with: {', '.join(required_vars)}")
    
    # Check scripts
    checks_total += 1
    scripts = ["generate_data.py", "simulate_realtime.py", "upload_to_gcs.py"]
    missing_scripts = [s for s in scripts if not (SCRIPTS_DIR / s).exists()]
    if not missing_scripts:
        logger.info(f"✓ All required scripts present")
        checks_passed += 1
    else:
        logger.warning(f"✗ Missing scripts: {', '.join(missing_scripts)}")
    
    logger.info("")
    logger.info(f"Status: {checks_passed}/{checks_total} checks passed")
    
    if checks_passed == checks_total:
        logger.info("✓ Ready to run pipeline!")
        return 0
    else:
        logger.warning("⚠ Some checks failed - see above")
        return 1


@cli.command()
def help_me():
    """
    Display help and usage examples
    """
    help_text = """
    
╔═══════════════════════════════════════════════════════════════════════════════╗
║          E-COMMERCE GCP PIPELINE - ORCHESTRATOR GUIDE                         ║
╚═══════════════════════════════════════════════════════════════════════════════╝

📋 PIPELINE PHASES:
  Phase 1: Ingestion (this script)
    - Generate synthetic orders/clients/products
    - Simulate real-time stream to Pub/Sub
    - Upload to Cloud Storage
  
  Phase 2: Processing (Dataflow - NOW IMPLEMENTED)
    - Apache Beam job to clean & transform data
    - Validate & enrich orders
    - Write clean data to BigQuery
  
  Phase 3: Storage (BigQuery)
    - Load cleaned data into data warehouse
  
  Phase 4: Visualization (Looker Studio)
    - Create interactive dashboard

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🚀 QUICK START:

  1. Setup GCP authentication:
     $ gcloud auth application-default login

  2. Create .env file with:
     PROJECT_ID=your-gcp-project
     PUBSUB_TOPIC=orders-realtime

  3. Check status:
     $ python main.py status

  4. Generate test data:
     $ python main.py generate --rows 1000

  5. Run full pipeline:
     $ python main.py pipeline

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📊 INDIVIDUAL COMMANDS:

  $ python main.py generate              # Generate synthetic data
  $ python main.py stream                # Publish to Pub/Sub (200 msgs)
  $ python main.py stream --limit 500    # Publish 500 messages
  $ python main.py stream --verbose      # Show message payloads
  $ python main.py upload --bucket gs://my-bucket  # Upload to GCS
  $ python main.py pipeline              # Run all phases at once
  $ python main.py status                # Check configuration

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

💰 COST CONTROL:

  Pub/Sub Pricing:
    - Free tier: 10GB/month
    - Each message: ~500 bytes
    - 200 messages: ~100KB (SAFE ✓)
    - 10,000 messages: ~5MB (SAFE ✓)
    - Never use --limit 0 (unlimited) in production

  Cloud Storage:
    - First 5GB/month: FREE
    - Raw data files: ~1-2MB total

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📁 PROJECT STRUCTURE:

  data/
    raw/             ← Raw generated data (orders.csv, clients.csv, etc.)
    clean/           ← Processed data (Dataflow output)
  
  scripts/
    generate_data.py ← Faker: Generate synthetic data
    simulate_realtime.py ← Pub/Sub: Stream simulator
    upload_to_gcs.py ← Cloud Storage: Upload files
  
  sql/
    create_tables.sql    ← BigQuery DDL
    create_views.sql     ← Aggregation views
  
  main.py          ← This orchestrator (CLI)
  requirements.txt ← Python dependencies

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

❓ TROUBLESHOOTING:

  • "Pub/Sub topic not found"
    → Create topic: gcloud pubsub topics create orders-realtime

  • "Data file not found"
    → Run: python main.py generate

  • "GCP authentication failed"
    → Run: gcloud auth application-default login

  • Environment variables not found
    → Create .env file in project root with PROJECT_ID, PUBSUB_TOPIC

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    """
    click.echo(help_text)


if __name__ == "__main__":
    sys.exit(cli())
