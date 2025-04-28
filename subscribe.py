import time
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1

# Your settings
project_id = "data-transport-lab1"
subscription_id = "my-subscription"
#timeout = 10.0  # Number of seconds to listen (can make it longer)

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

received_messages = 0
start_time = None
finished = False  # <--- add this

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    global received_messages, start_time, finished

    if received_messages == 0:
        start_time = time.time()

    received_messages += 1
    message.ack()

    if received_messages % 10000 == 0:
        print(f"Received {received_messages} messages...")

    if not finished and received_messages >= 462086:
        finished = True  # <--- prevent multiple prints
        end_time = time.time()
        print(f"Finished receiving {received_messages} messages.")
        print(f"Time taken to receive: {end_time - start_time:.2f} seconds")
        streaming_pull_future.cancel()
# Start listening
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}...\n")

with subscriber:
    try:
        streaming_pull_future.result()
    except TimeoutError:
        streaming_pull_future.cancel()
        streaming_pull_future.result()
