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
# 2. python3 txlog_subscriber_seek_target_sample.py -p <subscription_path> -t <timestamp_type> -s <timestamp>
# 3. example: 
#    python3 txlog_subscriber_seek_target_sample.py -p projects/123/locations/us-west1-a/subscriptions/my_subscription -t PUBLISH -s "2022-02-02 15:40:00"
#    python3 txlog_subscriber_seek_target_sample.py -p projects/123/locations/us-west1-a/subscriptions/my_subscription -t EVENT -s "2022-02-02 15:40:00"
#    python3 txlog_subscriber_seek_target_sample.py -p projects/123/locations/us-west1-a/subscriptions/my_subscription -t BEGIN
#    python3 txlog_subscriber_seek_target_sample.py -p projects/123/locations/us-west1-a/subscriptions/my_subscription -t END


import argparse
from datetime import datetime
from google.api_core.exceptions import NotFound
from google.cloud.pubsublite import AdminClient
from google.cloud.pubsublite.types import CloudRegion, CloudZone, SubscriptionPath, BacklogLocation, PublishTime, EventTime

# Sample Python SeekModifier to Netskope Transaction Events.
def move_cursor(project_number, cloud_region, zone_id, subscription_id, timestamp_type, timestamp):
    try:
        if timestamp_type == 'BEGIN':
            ts = BacklogLocation.BEGINNING
        elif timestamp_type == 'END':
            ts = BacklogLocation.END
        elif timestamp_type in ('PUBLISH', 'EVENT'):
            dt = datetime.fromisoformat(timestamp)
            ts = PublishTime(dt) if timestamp_type == 'PUBLISH' else EventTime(dt)
        else:
            print("invalid input timestamp, see --help")
    except:
        print("invalid input timestamp, see --help")
        return

    print(f"try to move seek cursor to {ts}")
    location = CloudZone(CloudRegion(cloud_region), zone_id)
    subscription_path = SubscriptionPath(
        project_number, location, subscription_id)

    client = AdminClient(cloud_region)
    try:
        # Initiate an out-of-band seek for a subscription to the specified
        # target. If an operation is returned, the seek has been successfully
        # registered and will eventually propagate to subscribers.
        seek_operation = client.seek_subscription(subscription_path, ts)
        print(f"Seek operation: {seek_operation.operation.name}")
    except NotFound:
        print(f"{subscription_path} not found.")
        return

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
subscription_path is the first argument
timestamp_type is the second argument
timestamp_type value could be:
  - 'BEGIN': replays from the beginning of all retained messages.
  - 'END': skips past all current published messages.
  - 'PUBLISH': PublishTime('<datetime>'), delivers messages with publish time greater than or equal to the specified timestamp.
  - 'EVENT': EventTime('<datetime>'), seeks to the first message with event time greater than or equal to the specified timestamp.
timestamp is the last argument which is required if the second argument is 'PUBLISH' or 'EVENT'
  - timestamp in format 'YYYY-MM-DD HH:MM:SS.fff, (.fff) is optional, e.g.'2011-11-04 00:05:23'
""")

    parser.add_argument(
        '-p', '--subscription_path', required=True, help="Transaction event subscription path")
    parser.add_argument(
        '-t', '--timestamp_type', required=True, choices=['BEGIN', 'END', 'PUBLISH', 'EVENT'],
        help="Transaction event Timestamp Type")
    parser.add_argument(
        '-s', '--timestamp', type=str,
        help="timestamp in format 'YYYY-MM-DD HH:MM:SS.fff, (.fff) is optional, e.g.'2011-11-04 00:05:23'")

    args = parser.parse_args()
    if args.subscription_path and args.timestamp_type:
        parts = args.subscription_path.split("/")
        location = parts[3].split("-")

        move_cursor(
            parts[1],
            location[0] + '-' + location[1],
            location[2],
            parts[5],
            args.timestamp_type,
            args.timestamp,
        )
    else:
        parser.print_help()
