import boto3

# Set your AWS credentials here
aws_access_key = 'YOUR AWS ACCESS KEY'
aws_secret_key = 'pYOU SECRET KEY'
region_name = 'us-east-1'  # Or your preferred region

# Create a session using the credentials
session = boto3.Session(
    aws_access_key_id="YOUR ACCESS KEY",
    aws_secret_access_key="YOUR SECRET ACCESS KEY",
    region_name="us-east-1"
)

# Initialize the Timestream client using the session
timestream_client = session.client('timestream-query')

# Example query to test connection and data retrieval
query = "SELECT * FROM \"TrafficMonitoringDB\".\"TrafficData\" LIMIT 15"
try:
    response = timestream_client.query(QueryString=query)
    print("Data retrieved:", response)
except Exception as e:
    print("Error fetching data:", e)
