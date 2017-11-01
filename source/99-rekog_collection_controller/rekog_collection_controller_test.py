import unittest
import mock
import json
import boto3

from rekog_collection_controller import RekognitionCollectionController
#from lambda_function import lambda_handler
from moto import mock_s3, mock_dynamodb2, mock_dynamodb
import logging

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)

RVA_COLLECTION_CONTROL_TABLE = "RVA_COLLECTION_CONTROL"

'''
Mocking for some operations in Moto doesnt work, so using DDB directly. 
May be converted to Local DDB in the future.
'''

class TestActor(unittest.TestCase):

    def test_id_generator(self):
        rcc = RekognitionCollectionController(RVA_COLLECTION_CONTROL_TABLE, '')
        rcc.set_collection_prefix("TEST")
        id = rcc.id_generator()

        assert id.startswith("TEST")

    #@mock_dynamodb2
    @mock.patch('rekog_collection_controller.RekognitionCollectionController.create_collection_call', return_value = {'StatusCode': 200, 'CollectionArn': 'string' })
    @mock.patch('rekog_collection_controller.RekognitionCollectionController.id_generator', return_value = "DVA-123456")
    def test_no_record_in_control_table(self, create_coll, id_gen):

        dynamodb = boto3.resource('dynamodb')
        dynamodb_client = boto3.client('dynamodb')

        #create_RVA_COLLECTION_CONTROL_TABLE(dynamodb)

        print("\n\n##test_no_record_in_control_table\n" )

        r  = dynamodb_client.delete_item(
            TableName=RVA_COLLECTION_CONTROL_TABLE,
            Key={"Identifier": {"S": "COLLECTIONS_NEW"}},
            ReturnConsumedCapacity='TOTAL'
        )

        rcc = RekognitionCollectionController(RVA_COLLECTION_CONTROL_TABLE, 'COLLECTIONS_NEW')
        collection_id = rcc.fetch_collection()

        print("COLL ID: {}".format(collection_id))

        r  = dynamodb_client.get_item(
            TableName=RVA_COLLECTION_CONTROL_TABLE,
            Key={"Identifier": {"S": "COLLECTIONS_NEW"}},
            ConsistentRead=True,
            ReturnConsumedCapacity='TOTAL'
        )

        item = r['Item']

        assert "DVA-123456" == item['Current']['S'] 
        assert "DVA-123456" == collection_id
        assert 0 == int(item['Count']['N'])
    

    #@mock_dynamodb2
    @mock.patch('rekog_collection_controller.RekognitionCollectionController.create_collection_call', return_value = {'StatusCode': 200, 'CollectionArn': 'string' })
    def test_control_table_less_than_max_size(self, call):

        dynamodb = boto3.resource('dynamodb')
        dynamodb_client = boto3.client('dynamodb')

        #create_RVA_COLLECTION_CONTROL_TABLE(dynamodb)
        
        dynamodb_client.update_item(
            TableName=RVA_COLLECTION_CONTROL_TABLE,
            Key={"Identifier": {"S": "COLLECTIONS_LESS"}},
            UpdateExpression="SET #curr = :newvalue, #cnt = :count, #cols = :coll_id", #list_append(if_not_exists(#cols, :empty_list), :coll_id)",        
            ExpressionAttributeNames={
                '#curr': 'Current',
                '#cnt': 'Count',
                '#cols': 'CollectionIds'
            },
            ExpressionAttributeValues={
                ':newvalue': {'S': "DVA-123456"}, 
                ':count': {'N': "100"},
                ':coll_id': {'L': [ { "S" : "DVA-123456" } ] }
            }
        )

        rcc = RekognitionCollectionController(RVA_COLLECTION_CONTROL_TABLE, 'COLLECTIONS_LESS')
        collection_id = rcc.fetch_collection()
       
        r  = dynamodb_client.get_item(
            TableName=RVA_COLLECTION_CONTROL_TABLE,
            Key={"Identifier": {"S": "COLLECTIONS_LESS"}},
            ConsistentRead=True,
            ReturnConsumedCapacity='TOTAL'
        )

        item = r['Item']

        print("\n\n##test_control_table_less_than_max_size\n")
        print("\n\n>>>>>>\n\n\n")
        print(json.dumps(item))

        self.assertEqual("DVA-123456", item['Current']['S'], msg="Data written in DDB is wrong. Exp: {} Found: {}".format("DVA-123456", item['Current']['S']))
        self.assertEqual("DVA-123456", collection_id, msg="Data written in DDB is wrong. Exp: {} Found: {}".format("DVA-123456", collection_id))
        self.assertEqual(100, int(item['Count']['N']), msg="Data written in DDB is wrong. Exp: {} Found: {}".format(100, int(item['Count']['N'])))


    #@mock_dynamodb2
    @mock.patch('rekog_collection_controller.RekognitionCollectionController.create_collection_call', return_value = {'StatusCode': 200, 'CollectionArn': 'string' })
    @mock.patch('rekog_collection_controller.RekognitionCollectionController.id_generator', return_value = "DVA-999999")
    def test_control_table_greater_than_max_size(self, call, id_gen):
        dynamodb = boto3.resource('dynamodb')
        dynamodb_client = boto3.client('dynamodb')

        #create_RVA_COLLECTION_CONTROL_TABLE(dynamodb)
        
        dynamodb_client.update_item(
            TableName=RVA_COLLECTION_CONTROL_TABLE,
            Key={"Identifier": {"S": "COLLECTIONS_GT"}},
            UpdateExpression="SET #curr = :newvalue, #cnt = :count, #cols = :coll_id", #list_append(if_not_exists(#cols, :empty_list), :coll_id)",        
            ExpressionAttributeNames={
                '#curr': 'Current',
                '#cnt': 'Count',
                '#cols': 'CollectionIds'
            },
            ExpressionAttributeValues={
                ':newvalue': {'S': "DVA-000000"}, 
                ':count': {'N': "1000000000"},
                ':coll_id': {'L': [ { "S" : "DVA-000000" } ] }
            }
        )
        
        rcc = RekognitionCollectionController(RVA_COLLECTION_CONTROL_TABLE, 'COLLECTIONS_GT')
        collection_id = rcc.fetch_collection()

        r  = dynamodb_client.get_item(
            TableName=RVA_COLLECTION_CONTROL_TABLE,
            Key={"Identifier": {"S": "COLLECTIONS_GT"}},
            ConsistentRead=True,
            ReturnConsumedCapacity='TOTAL'
        )

        item = r['Item']

        print("\n\n>>>>>>\n\n\n")
        print(json.dumps(item))

        self.assertEqual("DVA-999999", item['Current']['S'], msg="Data written in DDB is wrong. Exp: {} Found: {}".format("DVA-999999", item['Current']['S']))
        self.assertEqual("DVA-999999", collection_id, msg="Data written in DDB is wrong. Exp: {} Found: {}".format("DVA-999999", collection_id))
        self.assertEqual(0, int(item['Count']['N']), msg="Data written in DDB is wrong. Exp: {} Found: {}".format(0, int(item['Count']['N'])))


    #@mock_dynamodb2
    @mock.patch('rekog_collection_controller.RekognitionCollectionController.create_collection_call', return_value = {'StatusCode': 200, 'CollectionArn': 'string' })
    @mock.patch('rekog_collection_controller.RekognitionCollectionController.id_generator', return_value = "DVA-999999")
    def test_get_collections(self, call, id_gen):
        dynamodb = boto3.resource('dynamodb')
        dynamodb_client = boto3.client('dynamodb')

        #create_RVA_COLLECTION_CONTROL_TABLE(dynamodb)
        
        dynamodb_client.update_item(
            TableName=RVA_COLLECTION_CONTROL_TABLE,
            Key={"Identifier": {"S": "COLLECTIONS_GET"}},
            UpdateExpression="SET #curr = :newvalue, #cnt = :count, #cols = :coll_id", #list_append(if_not_exists(#cols, :empty_list), :coll_id)",        
            ExpressionAttributeNames={
                '#curr': 'Current',
                '#cnt': 'Count',
                '#cols': 'CollectionIds'
            },
            ExpressionAttributeValues={
                ':newvalue': {'S': "DVA-000000"}, 
                ':count': {'N': "0"},
                ':coll_id': {'L': [ { "S" : "DVA-000000" }, { "S" : "DVA-000001" }, { "S" : "DVA-000002" } ] }
            }
        )
        
        rcc = RekognitionCollectionController(RVA_COLLECTION_CONTROL_TABLE, 'COLLECTIONS_GET')
        collection_ids = rcc.list_collections()

        print("\n\nGET COLL\n\n\n")
        print(collection_ids)

        self.assertEqual(3, len(collection_ids), msg="Data written in DDB is wrong. Exp: {} Found: {}".format(3, len(collection_ids)))


    #@mock_dynamodb2
    @mock.patch('rekog_collection_controller.RekognitionCollectionController.create_collection_call', return_value = {'StatusCode': 200, 'CollectionArn': 'string' })
    @mock.patch('rekog_collection_controller.RekognitionCollectionController.id_generator', return_value = "DVA-999999")
    def test_increment_collection_count(self, call, id_gen):
        dynamodb = boto3.resource('dynamodb')
        dynamodb_client = boto3.client('dynamodb')

        #create_RVA_COLLECTION_CONTROL_TABLE(dynamodb)
        
        dynamodb_client.update_item(
            TableName=RVA_COLLECTION_CONTROL_TABLE,
            Key={"Identifier": {"S": "COLLECTIONS_INC"}},
            UpdateExpression="SET #curr = :newvalue, #cnt = :count, #cols = :coll_id", #list_append(if_not_exists(#cols, :empty_list), :coll_id)",        
            ExpressionAttributeNames={
                '#curr': 'Current',
                '#cnt': 'Count',
                '#cols': 'CollectionIds'
            },
            ExpressionAttributeValues={
                ':newvalue': {'S': "DVA-000000"}, 
                ':count': {'N': "1234"},
                ':coll_id': {'L': [ { "S" : "DVA-000000" }, { "S" : "DVA-000001" }, { "S" : "DVA-000002" } ] }
            }
        )
        
        rcc = RekognitionCollectionController(RVA_COLLECTION_CONTROL_TABLE, 'COLLECTIONS_INC')
        r = rcc.increment_collection_count("DVA-000000", 1000)

        print("\n\UPDATE COUNT\n\n\n")
        print(r)

        self.assertEqual(2234, r, msg="Data written in DDB is wrong. Exp: {} Found: {}".format("", ""))


def create_RVA_COLLECTION_CONTROL_TABLE(dynamodb):
    dynamodb.create_table(TableName=RVA_COLLECTION_CONTROL_TABLE,
                        KeySchema=[{'AttributeName': 'Identifier', 'KeyType': 'HASH'}],
                        AttributeDefinitions=[{'AttributeName': 'Identifier', 'AttributeType': 'S'}],
                        ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5})


if __name__ == '__main__':
    unittest.main()
