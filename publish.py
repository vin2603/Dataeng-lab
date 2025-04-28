import json
import requests
import time
from concurrent import futures
from google.oauth2 import service_account
from google.cloud import pubsub_v1
from datetime import datetime

# Settings

SERVICE_ACCOUNT_FILE = "data-transport-lab1-c01ef363636f.json"
PROJECT_ID = "data-transport-lab1"
TOPIC_ID = "my-topic"


# Load credentials and create publisher
pubsub_creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
publisher = pubsub_v1.PublisherClient(credentials=pubsub_creds)
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# List of 200 vehicle IDs
vehicle_ids = [
    2905, 2907, 2908, 2916, 2918, 2923, 2926, 2927, 2928, 2934,
    2935, 2939, 3008, 3010, 3013, 3017, 3018, 3019, 3023, 3033,
    3037, 3044, 3045, 3051, 3053, 3054, 3059, 3105, 3108, 3110,
    3113, 3114, 3118, 3126, 3127, 3128, 3130, 3143, 3145, 3148,
    3154, 3157, 3163, 3201, 3206, 3209, 3213, 3217, 3219, 3222,
    3224, 3225, 3229, 3231, 3233, 3239, 3243, 3245, 3246, 3248,
    3252, 3256, 3267, 3301, 3303, 3304, 3308, 3310, 3313, 3314,
    3320, 3326, 3327, 3401, 3404, 3405, 3407, 3411, 3419, 3420,
    3422, 3511, 3515, 3516, 3517, 3518, 3520, 3524, 3525, 3531,
    3533, 3534, 3538, 3542, 3544, 3545, 3547, 3554, 3559, 3560,
    3564, 3566, 3568, 3570, 3571, 3573, 3574, 3601, 3606, 3608,
    3615, 3623, 3627, 3629, 3631, 3632, 3646, 3650, 3704, 3710,
    3714, 3718, 3727, 3730, 3731, 3733, 3736, 3741, 3744, 3757,
    3902, 3903, 3909, 3911, 3912, 3915, 3916, 3921, 3927, 3929,
    3930, 3931, 3933, 3938, 3940, 3944, 3953, 3954, 3955, 3961,
    3963, 3964, 4002, 4003, 4006, 4014, 4015, 4016, 4018, 4020,
    4022, 4023, 4026, 4033, 4036, 4038, 4040, 4043, 4045, 4047,
    4051, 4058, 4059, 4066, 4067, 4069, 4202, 4203, 4206, 4207,
    4208, 4212, 4215, 4217, 4218, 4219, 4222, 4224, 4227, 4228,
    4230, 4231, 4234, 4305, 4510, 4515, 4516, 4522, 4526, 99222
]

# Callback for futures
def future_callback(future):
    try:
        future.result()
    except Exception as e:
        print(f"An error occurred: {e}")

# Start time
start_time = datetime.now()
print(f"[{start_time}] Gathering data and publishing to Pub/Sub...")

future_list = []
total_records = 0

for idx, vehicle_id in enumerate(vehicle_ids):
    url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        breadcrumbs = response.json()

        for record in breadcrumbs:
            message = json.dumps(record).encode()
            future = publisher.publish(topic_path, message)
            future.add_done_callback(future_callback)
            future_list.append(future)
            total_records += 1

            # Progress update every 10,000 messages
            if total_records % 10000 == 0:
                print(f"[{datetime.now()}] Published {total_records} records so far... (Current Vehicle ID: {vehicle_id})")
    except Exception as e:
        pass
        

gathering_done_time = datetime.now()
print(f"[{gathering_done_time}] Gathering complete.")
print(f"[{gathering_done_time}] Publishing remaining records...")

print("Waiting on Publisher futures...")

# Wait for all futures to complete
for future in futures.as_completed(future_list):
    continue

publish_done_time = datetime.now()

print(f"[{publish_done_time}] Publishing complete. Total records published: {total_records}")
print(f"Publish started at [{start_time}].")
print("Operation finished.")
