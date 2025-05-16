import boto3
import json
from collections import defaultdict
from azure.eventhub import EventHubProducerClient, EventData

# Azure Event Hub setup (hardcoded as requested)
EVENT_HUB_CONN_STR = "Endpoint=="
EVENT_HUB_NAME = "eventhubname"

# Clients
timestream_query = boto3.client('timestream-query')
event_hub_producer = EventHubProducerClient.from_connection_string(
    conn_str=EVENT_HUB_CONN_STR,
    eventhub_name=EVENT_HUB_NAME
)

def run_query():
    print("Running Timestream query...")
    query_string = """
        SELECT time, location, measure_name, measure_value::double, measure_value::bigint
        FROM TrafficMonitoringDB.TrafficData
        ORDER BY time DESC
        LIMIT 50
    """
    return timestream_query.query(QueryString=query_string)

def parse_rows(column_info, rows):
    records = defaultdict(dict)

    for i, row in enumerate(rows):
        data = row['Data']

        if len(data) < 5:  # Check if we have at least time, measure name, and measure value
            print(f"[WARNING] Skipping row {i} due to insufficient data: {data}")
            continue

        try:
            time_val = data[0].get('ScalarValue')
            location = data[1].get('ScalarValue')
            measure_name = data[2].get('ScalarValue')

            # Default values in case of missing measures
            val_double = data[3].get('ScalarValue') if 'ScalarValue' in data[3] else None
            val_bigint = data[4].get('ScalarValue') if 'ScalarValue' in data[4] else None

            # Make sure we have the required information (time, location, and at least one valid measure)
            if not time_val or not measure_name:
                print(f"[WARNING] Skipping row {i} due to missing time or measure_name: {data}")
                continue

            # Ensure location doesn't contain duplicate "Intersection_" parts
            if location:
                # Remove unwanted text such as (type: BIGINT) or (type: DOUBLE)
                location = location.split(' (')[0]
                # Clean up any extra "Intersection_" parts if needed
                if 'Intersection_' in location:
                    location_parts = location.split('Intersection_')  # Split at every "Intersection_"
                    location = 'Intersection_' + location_parts[-1]  # Take only the last part

            # Format the location as 'Intersection_X' if it doesn't match that format
            formatted_location = f"Intersection_{location}" if location else "Unknown"

            # Create the record for the timestamp
            record = records[time_val]
            record['time'] = time_val
            record['location'] = formatted_location  # Use the formatted location

            # Add measure data
            if measure_name == 'vehicle_count' and val_bigint is not None:
                record['vehicle_count'] = int(val_bigint)
            elif measure_name == 'average_speed' and val_double is not None:
                record['average_speed'] = float(val_double)

        except Exception as e:
            print(f"[ERROR] Failed to parse row {i}: {e}")
            print("Row data:", data)

    # Filter out incomplete records (those with missing vehicle count or average speed)
    return [rec for rec in records.values() if 'vehicle_count' in rec and 'average_speed' in rec]

def send_to_eventhub(records):
    if not records:
        print("No valid records to send.")
        return

    print(f"Sending {len(records)} records to Azure Event Hub...")
    events = [EventData(json.dumps(rec)) for rec in records]

    with event_hub_producer:
        event_hub_producer.send_batch(events)

    print("All records sent to Event Hub.")

def main():
    try:
        print("Fetching data from Timestream...")
        response = run_query()
        rows = response['Rows']
        column_info = response['ColumnInfo']

        # Parsing the rows and converting them into records
        records = parse_rows(column_info, rows)

        # Print parsed records to check
        for rec in records:
            print("Parsed record:", rec)

        # Send parsed records to Event Hub
        send_to_eventhub(records)

    except Exception as e:
        print("Fatal error:", str(e))

if __name__ == "__main__":
    main()
