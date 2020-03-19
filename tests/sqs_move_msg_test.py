from unittest.mock import Mock, patch, call
from moto import mock_sqs
import boto3
import unittest
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from sqs_msg_transfer import sqs_move_msg

source_queue_name = "source-queue"
destination_queue_name = "destination-queue"
region = "us-east-1"
batch=10
purge=False

class Sqs_move_msg_test(unittest.TestCase):

    @mock_sqs
    def setUp(self):
        sqs_resource = boto3.resource("sqs", region_name=region)
        queue = sqs_resource.create_queue(QueueName=source_queue_name)
        queue.send_message(MessageBody="testing")

    @mock_sqs
    def test_move_message(self):
        sqs = boto3.client("sqs", region_name="us-east-1")
        source_queue = sqs.create_queue(QueueName=source_queue_name)
        destination_queue = sqs.create_queue(QueueName=destination_queue_name)
        source_url = sqs.get_queue_url(QueueName=source_queue_name)
        print(source_url['QueueUrl'])
        sqs_move_msg.setup_logging()
        test_process_completion = sqs_move_msg.move_message(sqs,source_url['QueueUrl'],destination_queue['QueueUrl'],batch,region,purge)
        self.assertIs(test_process_completion, True)

if __name__ == "__main__":
    sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
    unittest.main()
