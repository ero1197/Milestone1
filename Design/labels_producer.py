from google.cloud import pubsub_v1
import csv
import glob
import json
import os
import time

json_files = glob.glob("*.json")
if not json_files:
    raise FileNotFoundError("No .json service account key found in this folder.")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_files[0]

project_id="industrial-gist-485800-j2";
topic_name = "labelsTopic";   
csv_file = "Labels.csv";

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Publishing to: {topic_path}")

published = 0

with open(csv_file, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        # Convert row (dict) to JSON string
        message_json = json.dumps(row)

        # Publish as bytes
        future = publisher.publish(topic_path, data=message_json.encode("utf-8"))
        message_id = future.result()

        published += 1
        print(f"[{published}] Published message_id={message_id} data={message_json}")

        # small delay so you can see it flow (optional)
        time.sleep(0.1)

print(f"Done. Published {published} rows from {CSV_FILE}.")