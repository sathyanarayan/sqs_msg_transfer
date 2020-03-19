# sqs_msg_transfer
Move all messages from source queue to destination queue 

# Dependencies:

1.boto 3

2.moto


# Liscence 
The MIT License (MIT)

Copyright (c) 2016-2018 Scott Barr

See LICENSE.md 

# Run 

```
python sqs_move_msg.py --help
usage: sqs_move_msg.py [-h] -s SOURCE [-d DEST] [-pr] -r REGION [-b BATCH]

Move messages between SQS queues.

optional arguments:
  -h, --help            show this help message and exit
  -s SOURCE, --source SOURCE
                        Source queue name
  -d DEST, --dest DEST  Destination queue name
  -pr, --purge          Purge the destination Queue
  -r REGION, --region REGION
                        The queue Regions in the order of source destination
  -b BATCH, --batch BATCH
                        The number of messages to request each iteration 10
                        maximum
```

## Sample Command :

```
python sqs_move_msg.py -s "https://eu-west-1.queue.amazonaws.com/<accountnumber>/testing-deals-queue-dlq" -d "https://eu-west-1.queue.amazonaws.com/<accountnumber>/testing-deals-queue-1" -r eu-west-1  -b 10
```
