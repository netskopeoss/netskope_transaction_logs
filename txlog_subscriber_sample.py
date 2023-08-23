# Copyright 2022 Netskope Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# To use:
# 1. export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
# 2. python txlog_subscriber_sample.py {subscription_path} {timeout}
# example: python txlog_subscriber_sample.py projects/123/locations/us-west1-a/subscriptions/my_subscription 600

import argparse
import datetime
from google.cloud.pubsublite.types import MessageMetadata
from google.pubsub_v1 import PubsubMessage
import gzip
import threading

g_print_lock = threading.Lock()

# Sample Python Subscriber to Netskope Transaction Events.
def receive_messages(
    project_number, cloud_region, zone_id, subscription_id, timeout=90
):
    from concurrent.futures._base import TimeoutError
    from google.cloud.pubsublite.cloudpubsub import SubscriberClient
    from google.cloud.pubsublite.types import (
        CloudRegion,
        CloudZone,
        FlowControlSettings,
        SubscriptionPath,
    )

    if zone_id:
        location = CloudZone(CloudRegion(cloud_region), zone_id)
    else:
        location = CloudRegion(cloud_region)

    subscription_path = SubscriptionPath(
        project_number, location, subscription_id)
    per_partition_flow_control_settings = FlowControlSettings(
        messages_outstanding=1000,
        bytes_outstanding=10 * 1024 * 1024,
    )

    def callback(message: PubsubMessage):
        # Due to thread safety, we need to accumulate the data and print at once
        # or output would be interleaved
        buffer = []
        metadata = MessageMetadata.decode(message.message_id)
        buffer.append(
            f"\n\nReceived msg at {datetime.datetime.now()} with partition {metadata.partition} offset {str(metadata.cursor).strip()}")
        buffer.append("Atrributes:")
        buffer.append(f"Content-Encoding: {message.attributes['Content-Encoding']}")
        buffer.append(f"Log-Count: {message.attributes['Log-Count']}")
        buffer.append(f"Fields: {message.attributes['Fields']}")
        # for testing purpose, some data may not be gzip data, hence pass that case
        try:
            event = gzip.decompress(message.data)
            buffer.append("Transaction event:")
            buffer.append(f"{event.decode('utf-8')}")
        except:
            pass
        message.ack()

        # Print out all msgs at once while holding the print lock
        with g_print_lock:
            for str in buffer:
                print(str)


    with SubscriberClient() as subscriber_client:

        streaming_pull_future = subscriber_client.subscribe(
            subscription_path,
            callback=callback,
            per_partition_flow_control_settings=per_partition_flow_control_settings,
        )

        print(
            f"Listening for transaction events on {str(subscription_path)}...")

        try:
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError or KeyboardInterrupt:
            streaming_pull_future.cancel()
            assert streaming_pull_future.done()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "subscription_path", help="Transaction event subscription path")
    parser.add_argument(
        "timeout",
        nargs="?",
        default=90,
        type=int,
        help="Timeout in second (default to 90s)",
    )

    args = parser.parse_args()
    parts = args.subscription_path.split("/")
    location = parts[3].split("-")

    receive_messages(
        parts[1],
        location[0] + '-' + location[1],
        location[2] if len(location) > 2 else None,
        parts[5],
        args.timeout,
    )

