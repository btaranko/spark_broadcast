"""
Test local kinesis stream. create/write/read/delete
https://blog.ruanbekker.com/blog/2019/06/22/play-with-kinesis-data-streams-for-free/


"""
import boto3
import json

client = boto3.Session(region_name='eu-west-1').\
    client('kinesis', aws_access_key_id='', aws_secret_access_key='', endpoint_url='http://localhost:4567')

# print(client.list_streams())

# create stream 'borstream' with 1 primary shard
# client.create_stream(StreamName='borstream', ShardCount=1)
# print(client.list_streams())

list_streams = client.list_streams()
if 'borstream' not in list_streams['StreamNames']:
    client.create_stream(StreamName='borstream', ShardCount=1)

# put some Data in our kinesis stream with partition key a01 which is used for sharding
response = client.put_record(StreamName='borstream', Data=json.dumps({"name": "boris"}), PartitionKey='a01')

# Before data can be read from the stream we need to obtain the shard iterator for the shard we are interested in.
# There are 2 common iterator types:
#     TRIM_HORIZON: Points to the last untrimmed record in the shard
#     LATEST: Reads the most recent data in the shard
shard_id = response['ShardId']
response = client.get_shard_iterator(StreamName='borstream', ShardId=shard_id, ShardIteratorType='TRIM_HORIZON')

# read data from stream
shard_iterator = response['ShardIterator']
response = client.get_records(ShardIterator=shard_iterator)

for record in response['Records']:
    if 'Data' in record:
        print(json.loads(record['Data']))

# print(response)

# delete stream
client.delete_stream(StreamName='borstream')
