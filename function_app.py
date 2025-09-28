import logging
import requests
import json
import os
from datetime import datetime, timezone
from azure.storage.blob import BlobServiceClient
import azure.functions as func

# --- Configuration ---
# These values will be read from the Function App's settings
CONNECTION_STRING = os.environ["AZURE_STORAGE_CONNECTION_STRING"]

# The container and folder path where Databricks is looking for files
# This should match your DLT pipeline's source path
BLOB_CONTAINER_NAME = "wikipedia-data" # New container for Wikipedia data
BLOB_FOLDER_PATH = "landing/raw_files" # The folder path inside the container

# How many updates to fetch per run
UPDATES_TO_FETCH = 15


app = func.FunctionApp()

@app.schedule(schedule="0 */15 * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def get_wikipedia_updates(myTimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    # 1. Initialize Blob Service Client
    try:
        blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        logging.info("Successfully connected to Blob Storage.")
    except Exception as e:
        logging.error(f"Failed to connect to Blob Storage: {e}")
        return

    # 2. Fetch data from Wikipedia Stream
    events = []
    try:
        headers = {
            'User-Agent': 'DLTWikipediaBot/1.0 (https://deus.ai; pablo.formoso@deus.ai)'
        }
        with requests.get("https://stream.wikimedia.org/v2/stream/recentchange", 
                         stream=True, timeout=30, headers=headers) as response:
            response.raise_for_status()
            logging.info(f"Connected to Wikipedia stream. Fetching {UPDATES_TO_FETCH} events.")
            
            for line in response.iter_lines():
                if line.startswith(b'data: '):
                    event = json.loads(line.decode('utf-8')[6:])
                    events.append(event)
                    if len(events) >= UPDATES_TO_FETCH:
                        break
        logging.info(f"Successfully fetched {len(events)} events.")
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch data from Wikipedia: {e}")
        return

    # 3. Write fetched data to a new blob in Azure Storage
    if events:
        try:
            # Create a unique filename
            timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            file_name = f"{BLOB_CONTAINER_NAME}/wiki_edits_{timestamp_str}.json"
            
            # Convert the list of events to a JSON string (one JSON object per line)
            file_content = "\n".join(json.dumps(event) for event in events)
            
            # Get a client to interact with the specific blob
            blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER_NAME, blob=file_name)
            
            # Upload the data
            blob_client.upload_blob(file_content, overwrite=True)
            logging.info(f"Successfully uploaded data to: {file_name}")

        except Exception as e:
            logging.error(f"Failed to upload blob: {e}")

    logging.info('Function execution complete.')