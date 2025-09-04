"""
FDA Food Enforcement Connector

This connector retrieves food enforcement data from the openFDA API.
The data includes food recall information, distribution patterns, and enforcement actions.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# For making HTTP requests to the FDA API
import requests

# For handling dates and timezone operations
from datetime import datetime, timezone

# For handling time delays and rate limiting
import time

# For flattening nested dictionaries
from typing import Dict, List, Any, Optional

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    # Validate required configuration parameters
    required_configs = ["api_key"]
    for key in required_configs:
        if key not in configuration or not configuration[key]:
            raise ValueError(f"Missing required configuration value: {key}")


def flatten_dict(d: dict, parent_key: str = '', sep: str = '_') -> dict:
    """
    Flatten a nested dictionary by joining keys with a separator.
    This function recursively flattens nested dictionaries to create flat key-value pairs.
    Args:
        d: Dictionary to flatten
        parent_key: Parent key for nested dictionaries
        sep: Separator to use between keys
    Returns:
        Flattened dictionary
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            # Convert lists to JSON strings for storage
            items.append((new_key, json.dumps(v) if v else None))
        else:
            items.append((new_key, v))
    return dict(items)


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "food_enforcement_records",  # Name of the table in the destination, required.
            "primary_key": ["recall_number"],  # Primary key column(s) for the table, optional.
            # Let Fivetran infer column types from the data
        },
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.info("FDA Food Enforcement Connector: Starting sync")

    # Validate the configuration to ensure it contains all required values.
    validate_configuration(configuration=configuration)

    # Extract configuration parameters
    api_key = configuration.get("api_key")
    limit = int(configuration.get("limit", "1000"))  # Default to 1000, max allowed by FDA API
    max_records = int(configuration.get("max_records", "10000"))  # Default max records per sync
    sync_mode = configuration.get("sync_mode", "incremental")
    
    # FDA API base URL
    base_url = "https://api.fda.gov/food/enforcement.json"
    
    # Initialize state variables
    if not state:
        state = {
            "last_sync_date": None,
            "total_processed": 0,
            "last_cursor": None
        }
    
    last_sync_date = state.get("last_sync_date")
    total_processed = state.get("total_processed", 0)
    
    try:
        # Build API request parameters
        params = {
            "api_key": api_key,
            "limit": min(limit, 1000)  # FDA API max limit is 1000
        }
        
        # Handle incremental sync with date filtering
        if sync_mode == "incremental" and last_sync_date:
            # Search for records updated since last sync
            params["search"] = f'report_date:[{last_sync_date}+TO+*]'
            log.info(f"Performing incremental sync since: {last_sync_date}")
        else:
            log.info("Performing full sync")
        
        skip = 0
        current_batch_processed = 0
        
        while total_processed < max_records:
            # Add skip parameter for pagination
            if skip > 0:
                params["skip"] = skip
            
            log.info(f"Fetching batch: skip={skip}, limit={params['limit']}")
            
            # Make API request with retry logic
            response = make_api_request_with_retry(base_url, params)
            
            if not response:
                log.warning("No response received from FDA API")
                break
            
            data = response.json()
            
            # Check if we have results
            if not data.get("results"):
                log.info("No more results found")
                break
            
            # Process the batch of records
            batch_processed = process_food_enforcement_records(data["results"])
            current_batch_processed += batch_processed
            total_processed += batch_processed
            
            log.info(f"Processed {batch_processed} records in this batch. Total: {total_processed}")
            
            # Check if we've reached the end
            if len(data["results"]) < params["limit"]:
                log.info("Reached end of available data")
                break
            
            # Update skip for next iteration
            skip += params["limit"]
            
            # Checkpoint after each batch to ensure progress is saved
            # Use the latest report_date from the current batch for incremental sync
            latest_report_date = get_latest_report_date(data["results"])
            new_state = {
                "last_sync_date": latest_report_date,
                "total_processed": total_processed,
                "last_cursor": skip
            }
            op.checkpoint(new_state)
            
            # Rate limiting - FDA API allows 240 requests per minute
            # Sleep for 0.25 seconds to stay under the limit
            time.sleep(0.25)
        
        # Final checkpoint
        # Use the latest report_date from the last batch for incremental sync
        if 'data' in locals() and data.get("results"):
            latest_report_date = get_latest_report_date(data["results"])
        else:
            # Fallback to current time if no data was processed
            latest_report_date = datetime.now(timezone.utc).isoformat()
        
        final_state = {
            "last_sync_date": latest_report_date,
            "total_processed": total_processed,
            "last_cursor": skip
        }
        op.checkpoint(final_state)
        
        log.info(f"Sync completed successfully. Total records processed: {total_processed}")
        
    except Exception as e:
        # In case of an exception, log the error and raise a runtime error
        log.severe(f"Failed to sync FDA Food Enforcement data: {str(e)}")
        raise RuntimeError(f"Failed to sync data: {str(e)}")


def get_latest_report_date(records: List[Dict[str, Any]]) -> str:
    """
    Extract the latest report_date from a batch of records.
    This is used for proper incremental sync cursor management.
    Args:
        records: List of FDA API records
    Returns:
        Latest report_date as ISO string, or current time if no valid dates found
    """
    latest_date = None
    
    for record in records:
        report_date = record.get("report_date")
        if report_date:
            # FDA dates are in YYYYMMDD format, convert to ISO
            try:
                if len(report_date) == 8:  # YYYYMMDD format
                    year, month, day = report_date[:4], report_date[4:6], report_date[6:8]
                    iso_date = f"{year}-{month}-{day}T00:00:00Z"
                    if latest_date is None or iso_date > latest_date:
                        latest_date = iso_date
            except (ValueError, IndexError):
                continue
    
    # Return latest date found, or current time as fallback
    return latest_date if latest_date else datetime.now(timezone.utc).isoformat()


def make_api_request_with_retry(base_url: str, params: dict, max_retries: int = 3) -> Optional[requests.Response]:
    """
    Make an API request with retry logic to handle transient errors.
    Args:
        base_url: The base URL for the API request
        params: Parameters for the API request
        max_retries: Maximum number of retry attempts
    Returns:
        Response object if successful, None if all retries failed
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            return response
            
        except requests.exceptions.RequestException as e:
            log.warning(f"API request failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                # Exponential backoff: wait 2^attempt seconds
                wait_time = 2 ** attempt
                log.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                log.severe(f"All retry attempts failed for API request")
                raise
    
    return None


def process_food_enforcement_records(records: List[Dict[str, Any]]) -> int:
    """
    Process a batch of food enforcement records and upsert them.
    This function flattens nested dictionaries and upserts the data.
    Args:
        records: List of food enforcement records from the API
    Returns:
        Number of records processed
    """
    processed_records = []
    
    for record in records:
        try:
            # Flatten the record to handle nested dictionaries
            flattened_record = flatten_dict(record)
            
            # Add metadata fields
            flattened_record["_fivetran_synced"] = datetime.now(timezone.utc).isoformat()
            flattened_record["_fivetran_deleted"] = False
            
            processed_records.append(flattened_record)
            
        except Exception as e:
            log.warning(f"Failed to process record {record.get('recall_number', 'unknown')}: {str(e)}")
            continue
    
    # Upsert all processed records
    if processed_records:
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The op.upsert method is called with two arguments:
        # - The first argument is the name of the table to upsert the data into.
        # - The second argument is a list of dictionaries containing the data to be upserted.
        for record in processed_records:
            op.upsert(table="food_enforcement_records", data=record)
        log.info(f"Upserted {len(processed_records)} food enforcement records")
    
    return len(processed_records)


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
