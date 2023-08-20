import requests
import json

# Define the API endpoint URL
url = 'http://localhost:5001/send_to_kafka'  # Adjust the URL as needed

# Define the API key
api_key = 'Wt_opsKafka12if'  # Replace with an incorrect API key

# Define the JSON data to send in the request body
data = {
    "order_id": "1234jj5",
    "pg_order_id": "67kk890"
}

# Set headers including the Authorization header with the API key
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {api_key}"
}

# Send the POST request
response = requests.post(url, data=json.dumps(data), headers=headers)

# Check the response status code and handle it accordingly
if response.status_code == 401:
    print("Unauthorized - Check your API key.")
else:
    print("Request failed with status code:", response.status_code)
    print(response.json())
