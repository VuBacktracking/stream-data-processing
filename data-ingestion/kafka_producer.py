import argparse
import io
import json
from datetime import datetime
from time import sleep

import numpy as np
from bson import json_util
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic

parser = argparse.ArgumentParser()
parser.add_argument(
    "-m",
    "--mode",
    default="setup",
    choices=["setup", "teardown"],
    help="Whether to setup or teardown a Kafka topic with driver stats events. Setup will teardown before beginning emitting events.",
)
parser.add_argument(
    "-b",
    "--bootstrap_servers",
    default="localhost:9092",
    help="Where the bootstrap server is",
)
parser.add_argument(
    "-c",
    "--schemas_path",
    default="./avro_schemas",
    help="Folder containing all generated avro schemas",
)

args = parser.parse_args()

# Define some constants
NUM_PRODUCTS = 1

def create_topic(admin, topic_name):
    # Create topic if not exists
    try:
        # Create Kafka topic
        topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin.create_topics([topic])
        print(f"A new topic {topic_name} has been created!")
    except Exception:
        print(f"Topic {topic_name} already exists. Skipping creation!")
        pass

def create_streams(servers, schemas_path):
    producer = None
    admin = None
    for _ in range(10):
        try:
            producer = KafkaProducer(bootstrap_servers=servers)
            admin = KafkaAdminClient(bootstrap_servers=servers)
            print("Success: instantiated Kafka admin and producer.")
        except Exception as e:
            print(
                f"Trying to instantiate admin and producer with bootstrap servers {servers} with error {e}"
            )
            sleep(10)
            pass

        while True:
            data = {}
            # Make event one more year recent to simulate fresher data
            data["created"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            data["product_id"] = np.random.randint(low=0, high=NUM_PRODUCTS)
            
            schema_path = f"{schemas_path}/products_schema_{data['product_id']}.avsc"
            with open(schema_path, "r") as f:
                parsed_schema = json.loads(f.read())
                
            for field in parsed_schema["fields"]:
                if field["name"] not in ["created", "product_id"]:
                    data[field["name"]] = np.random.rand()
                    
            
            record = {
                "schema": {"type" : "struct", "fields": parsed_schema["fields"]},
                "payload": data,
            }
            
            topic_name = "product_input"
            
            create_topic(admin = admin, topic_name=topic_name)
            
            # Send message to the topic
            producer.send(
                topic_name, json.dumps(record, default = json_util.default).encode("utf-8")
            )
            print(record)
            sleep(2)

def teardown_stream(topic_name, server = ["localhost:9092"]):
    try:
        admin = KafkaAdminClient(bootstrap_servers = server)
        print(admin.delete_topics([topic_name]))
        print(f"Topic {topic_name} deleted")
    except Exception as e:
        print(str(e))
        pass
    

if __name__ == "__main__":
    parsed_args = vars(args)
    mode = parsed_args["mode"]
    servers = parsed_args["bootstrap_servers"]
    
    # Tear down all previous streams
    print("Tearing down all existing topics!")
    for device_id in range(NUM_PRODUCTS):
        try:
            teardown_stream(f"device_{device_id}", [servers])
        except Exception as e:
            print(f"Topic device_{device_id} does not exist. Skipping...!")

    if mode == "setup":
        schemas_path = parsed_args["schemas_path"]
        create_streams([servers], schemas_path)