# FDA Food Enforcement Connector

A Fivetran Connector SDK implementation for retrieving food enforcement data from the openFDA API. This connector provides access to FDA recall information, distribution patterns, and enforcement actions from the FDA Recall Enterprise System (RES).

## Connector overview

The FDA Food Enforcement Connector retrieves data from the [openFDA Food Enforcement API](https://open.fda.gov/apis/food/enforcement/), which contains records from 2004 to the present, updated weekly. The data includes:

- Food recall information and classifications
- Distribution patterns and geographic data
- Recalling firm details and contact information
- Product descriptions and quantities
- Recall status and termination information
- Enforcement action details

The connector dynamically flattens nested JSON structures and creates a single table with all available fields, allowing Fivetran to infer column types automatically.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements) (3.9-3.12)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- **Dynamic Schema**: Automatically flattens nested JSON structures from the FDA API
- **Incremental Sync**: Supports incremental data synchronization based on report dates
- **Rate Limiting**: Respects FDA API rate limits (240 requests per minute with API key)
- **Error Handling**: Comprehensive error handling with retry logic and exponential backoff
- **Pagination**: Handles large datasets with automatic pagination
- **State Management**: Maintains sync state for reliable incremental updates
- **Data Validation**: Validates configuration and handles malformed data gracefully

## Configuration file

The connector requires the following configuration parameters in `configuration.json`:

```json
{
  "api_key": "YOUR_FDA_API_KEY_HERE",
  "limit": "1000",
  "max_records": "10000",
  "sync_mode": "incremental"
}
```

**Note**: A template configuration file `configuration.json.template` is provided. Copy it to `configuration.json` and update with your actual API key.

### Configuration Parameters

- **api_key** (required): Your FDA API key. Get your free key at [open.fda.gov](https://open.fda.gov/apis/)
- **limit** (optional): Number of records to fetch per API request (max 1000, default 1000)
- **max_records** (optional): Maximum total records to process in a single sync (default 10000)
- **sync_mode** (optional): Sync mode - "incremental" or "full" (default "incremental")

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

The `requirements.txt` file specifies the Python libraries required by the connector:

```
urllib3==2.0.7
certifi==2023.11.17
python-dateutil==2.8.2
jsonschema==4.20.0
httpx==0.25.2
pytz==2023.3
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The connector uses API key authentication to access the openFDA API. Here's how to obtain and use your API key:

### Getting an API Key

1. Visit [open.fda.gov/apis/](https://open.fda.gov/apis/)
2. Click "Get your API key"
3. Fill out the registration form
4. You'll receive your API key via email

### API Rate Limits

- **Without API Key**: 1,000 requests per day, per IP address
- **With API Key**: 240 requests per minute, 120,000 requests per day

The connector automatically implements rate limiting to respect these limits.

## Pagination

The connector handles pagination automatically using the FDA API's `skip` and `limit` parameters. It processes data in batches and checkpoints progress after each batch to ensure reliable syncs.

Refer to lines 120-180 in `connector.py` for pagination implementation.

## Data handling

The connector processes data through the following steps:

1. **API Request**: Makes HTTP requests to the FDA Food Enforcement API
2. **Data Flattening**: Flattens nested JSON structures using the `flatten_dict` function
3. **Data Transformation**: Adds metadata fields (`_fivetran_synced`, `_fivetran_deleted`)
4. **Upsert Operation**: Uses `op.upsert` to insert or update records in the destination

Refer to the `process_food_enforcement_records` function (lines 200-230) for data processing logic.

## Error handling

The connector implements comprehensive error handling strategies:

- **Retry Logic**: Automatic retry with exponential backoff for transient errors
- **Rate Limit Handling**: Built-in delays to respect API rate limits
- **Data Validation**: Validates configuration and handles malformed records
- **Logging**: Detailed logging for debugging and monitoring

Refer to the `make_api_request_with_retry` function (lines 180-200) for error handling implementation.

## Tables created

The connector creates a single table that dynamically adapts to the FDA API structure:

### food_enforcement_records

**Primary Key**: `recall_number`

**Key Fields** (automatically inferred by Fivetran):
- `recall_number`: Unique recall identifier
- `recalling_firm`: Name of the recalling firm
- `product_description`: Description of the recalled product
- `reason_for_recall`: Reason for the recall
- `classification`: Recall classification (Class I, II, or III)
- `status`: Recall status (On-Going, Completed, Terminated, Pending)
- `distribution_pattern`: Geographic distribution pattern
- `recall_initiation_date`: Date recall was initiated
- `report_date`: Date the report was filed
- `country`, `state`, `city`: Geographic information
- `openfda_*`: Flattened OpenFDA metadata fields
- `_fivetran_synced`: Timestamp when record was synced
- `_fivetran_deleted`: Deletion marker (always false for this connector)

The connector flattens all nested structures, so fields like `openfda.brand_name` become `openfda_brand_name` in the destination table.

## Testing

### CLI Testing

Test the connector using the Fivetran CLI:

```bash
fivetran debug --configuration configuration.json
```

### Python Testing

Test the connector directly in Python:

```bash
python connector.py
```

### Expected Output

The connector will create a `warehouse.db` file with the `food_enforcement_records` table containing all FDA food enforcement data.

## Troubleshooting

### Common Issues

1. **Rate Limit Exceeded**
   - Solution: Ensure you have a valid FDA API key in your configuration
   - The connector includes built-in rate limiting

2. **No Data Returned**
   - Check your API key is valid and active
   - Verify network connectivity
   - Review the logs for specific error messages

3. **Sync State Issues**
   - For full resync, set `sync_mode` to "full" in configuration
   - Check the checkpoint logs for state information

### Log Analysis

The connector provides detailed logging:
- **INFO**: Status updates, batch processing, cursor information
- **WARNING**: Rate limit warnings, data processing issues
- **SEVERE**: API errors, critical failures

### Debug Mode

Enable debug mode by setting environment variable:
```bash
export FIVETRAN_DEBUG=true
```

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.

## References

- [Fivetran Connector SDK Documentation](https://fivetran.com/docs/connector-sdk)
- [FDA Food Enforcement API Documentation](https://open.fda.gov/apis/food/enforcement/)
- [SDK Examples Repository](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples)
- [openFDA API Basics](https://open.fda.gov/apis/)
- [FDA Recall Enterprise System](https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts)
