import json
import random
import time
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
from datetime import datetime

# AWS IoT Core configuration
mqtt_endpoint = "a3t3wq3hfabwnp-ats.iot.us-east-1.amazonaws.com"
mqtt_port = 8883
client_id = "TrafficSimulator_Random"
topic = "traffic/sensor"

# Certificate paths
root_ca_path = "C:/Users/Yasmine A S/OneDrive/Desktop/IOT TRAFFIC PROJECT/AmazonRootCA1.pem"
certificate_path = "C:/Users/Yasmine A S/OneDrive/Desktop/IOT TRAFFIC PROJECT/certificate.pem.crt"
private_key_path = "C:/Users/Yasmine A S/OneDrive/Desktop/IOT TRAFFIC PROJECT/private.pem.key"

# Create MQTT client
mqtt_client = AWSIoTMQTTClient(client_id)
mqtt_client.configureEndpoint(mqtt_endpoint, mqtt_port)
mqtt_client.configureCredentials(root_ca_path, private_key_path, certificate_path)

# Connect to AWS IoT Core
mqtt_client.connect()

# Define a pool of intersection names
intersections = [f"Intersection_{i}" for i in range(1, 11)]  # Intersection_1 to Intersection_10

# Function to format timestamp to human-readable format
def format_timestamp(epoch_time):
    # Convert the epoch time to a datetime object
    dt = datetime.fromtimestamp(epoch_time)
    # Format it as a human-readable string (e.g., "April 21, 2021, 10:28:00 PM")
    return dt.strftime("%B %d, %Y, %I:%M:%S %p")

# Continuously send traffic data from random intersections
while True:
    intersection = random.choice(intersections)
    
    # Ensure timestamp is a valid integer (Unix epoch time)
    timestamp = int(time.time())  # Get Unix timestamp as integer (epoch time)
    
    # Format timestamp to human-readable format
    human_readable_timestamp = format_timestamp(timestamp)

    data = {
        "location": intersection,
        "timestamp": timestamp,  # Store timestamp as Unix epoch time (integer)
        "human_readable_timestamp": human_readable_timestamp,  # Store human-readable timestamp as string
        "vehicle_count": random.randint(10, 120),
        "average_speed": round(random.uniform(20, 80), 2)
    }
    
    # Print the data to verify the timestamp format
    print(f"Publishing data from {intersection}: {data}")
    
    # Publish data to the IoT topic
    mqtt_client.publish(topic, json.dumps(data), 1)
    
    # Sleep for 3 seconds before publishing again
    time.sleep(3)
