"""
upload_to_gcs.py
Upload generated CSV files to Google Cloud Storage
"""

import os
import logging
from pathlib import Path
from google.cloud import storage

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Configuration
PROJECT_ID = "your-project-id"  # Replace with your GCP project ID
BUCKET_NAME = "ecommerce-data-lake"  # Replace with your bucket name
LOCAL_DATA_PATH = Path(__file__).parent / "data" / "raw"
GCS_PREFIX = "raw"  # Folder in bucket

def upload_to_gcs(project_id: str, bucket_name: str, local_dir: Path, gcs_prefix: str = ""):
    """
    Upload all CSV files from local directory to GCS bucket
    
    Args:
        project_id: GCP project ID
        bucket_name: Name of GCS bucket
        local_dir: Local directory containing CSV files
        gcs_prefix: Prefix (folder) in GCS bucket
    """
    try:
        # Initialize GCS client
        client = storage.Client(project=project_id)
        bucket = client.bucket(bucket_name)
        
        # Get all CSV files
        csv_files = list(local_dir.glob("*.csv"))
        
        if not csv_files:
            log.error(f"No CSV files found in {local_dir}")
            return False
        
        log.info(f"Found {len(csv_files)} CSV files to upload")
        
        for file_path in csv_files:
            # Create GCS path
            gcs_path = f"{gcs_prefix}/{file_path.name}" if gcs_prefix else file_path.name
            blob = bucket.blob(gcs_path)
            
            # Upload file
            log.info(f"Uploading {file_path.name} → gs://{bucket_name}/{gcs_path}")
            blob.upload_from_filename(str(file_path))
            
            # Get file size
            file_size_mb = file_path.stat().st_size / (1024 * 1024)
            log.info(f"✓ {file_path.name} ({file_size_mb:.2f} MB)")
        
        log.info(f"\n✓ All files uploaded to gs://{bucket_name}/{gcs_prefix}/")
        return True
        
    except Exception as e:
        log.error(f"Error uploading to GCS: {str(e)}")
        log.info("Make sure you have:")
        log.info("  1. Authenticated with: gcloud auth application-default login")
        log.info("  2. Created the bucket: gsutil mb gs://ecommerce-data-lake")
        log.info("  3. Updated PROJECT_ID and BUCKET_NAME in this script")
        return False


if __name__ == "__main__":
    upload_to_gcs(PROJECT_ID, BUCKET_NAME, LOCAL_DATA_PATH, GCS_PREFIX)
