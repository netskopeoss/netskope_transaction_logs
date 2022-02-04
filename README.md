**Copyright 2022 Netskope Inc**

# This repo contains transaction event logs related sample code

## 1. txlog_subscriber_sample.py

To use:

```shell
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
python txlog_subscriber_sample.py <subscription_path> <timeout>
```
Example:
```
python txlog_subscriber_sample.py projects/123/locations/us-west1-a/subscriptions/my_subscription 600
```

## 2. txlog_subscriber_seek_target_sample.py

To use:

```shell
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
python3 txlog_subscriber_seek_target_sample.py -p <subscription_path> -t <timestamp_type> -s <timestamp>
```
Examples: 
```shell
python3 txlog_subscriber_seek_target_sample.py -p projects/123/locations/us-west1-a/subscriptions/my_subscription -t PUBLISH -s "2022-02-02 15:40:00"

python3 txlog_subscriber_seek_target_sample.py -p projects/123/locations/us-west1-a/subscriptions/my_subscription -t EVENT -s "2022-02-02 15:40:00"

python3 txlog_subscriber_seek_target_sample.py -p projects/123/locations/us-west1-a/subscriptions/my_subscription -t BEGIN

python3 txlog_subscriber_seek_target_sample.py -p projects/123/locations/us-west1-a/subscriptions/my_subscription -t END
```
```shell
python3 txlog_subscriber_seek_target_sample.py --help
usage: txlog_subscriber_seek_target_sample.py [-h] -p SUBSCRIPTION_PATH -t {BEGIN,END,PUBLISH,EVENT} [-s TIMESTAMP]

optional arguments:
  -h, --help            show this help message and exit
  -p SUBSCRIPTION_PATH, --subscription_path SUBSCRIPTION_PATH
                        Transaction event subscription path
  -t {BEGIN,END,PUBLISH,EVENT}, --timestamp_type {BEGIN,END,PUBLISH,EVENT}
                        Transaction event Timestamp Type
  -s TIMESTAMP, --timestamp TIMESTAMP
                        timestamp in format 'YYYY-MM-DD HH:MM:SS.fff, (.fff) is optional, e.g.'2011-11-04 00:05:23'

subscription_path is the first argument
timestamp_type is the second argument
timestamp_type value could be:
  - 'BEGIN': replays from the beginning of all retained messages.
  - 'END': skips past all current published messages.
  - 'PUBLISH': PublishTime('<datetime>'), delivers messages with publish time greater than or equal to the specified timestamp.
  - 'EVENT': EventTime('<datetime>'), seeks to the first message with event time greater than or equal to the specified timestamp.
timestamp is the last argument which is required if the second argument is 'PUBLISH' or 'EVENT'
  - timestamp in format 'YYYY-MM-DD HH:MM:SS.fff, (.fff) is optional, e.g.'2011-11-04 00:05:23'

```

# References
https://docs.netskope.com/en/transaction-events.html
