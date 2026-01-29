from google.cloud import pubsub_v1
import glob
import json
import os

# ---- Auth: find JSON key in current folder ----
json_files = glob.glob("*.json")
if not json_files:
    raise FileNotFoundError("No .json service account key found in this folder.")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_files[0]

# ---- CONFIG: update these ----
project_id="industrial-gist-485800-j2";
subscription_id = "labelsTopic-sub";      

subscriber = pubsub_v1.SubscriberClient()
sub_path = subscriber.subscription_path(project_id, subscription_id)

print(f"Listening on: {sub_path} (Ctrl+C to stop)\n")

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        payload = message.data.decode("utf-8")
        row_dict = json.loads(payload)

        print("Received row:")
        for k, v in row_dict.items():
            print(f"  {k}: {v}")
        print("-" * 40)

        message.ack()
    except Exception as e:
        print(f"Error processing message: {e}")
        message.nack()

streaming_pull_future = subscriber.subscribe(sub_path, callback=callback)

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    print("\nStopped.")
