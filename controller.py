'''
This file consists of all the functions that are used to interact with the database.
It also consists of the code needed to connect to the dynamoDB database using the
configurations set in config.py file.
'''

from boto3 import resource
import config

AWS_ACCESS_KEY_ID = config.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY
REGION_NAME = config.REGION_NAME

resource = resource(
    'dynamodb',
    aws_access_key_id     = AWS_ACCESS_KEY_ID,
    aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
    region_name           = REGION_NAME
)

def create_table_movie():    
    table = resource.create_table(
        TableName = 'TachyonAssignment', # Name of the table 
        KeySchema = [
            {
                'AttributeName': 'id',
                'KeyType'      : 'HASH' # HASH = partition key, RANGE = sort key
            }
        ],
        AttributeDefinitions = [
            {
                'AttributeName': 'id', # Name of the attribute
                'AttributeType': 'N'   # N = Number (S = String, B= Binary)
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits'  : 10,
            'WriteCapacityUnits': 10
        }
    )
    return table

MovieTable = resource.Table('Movie')

def write_to_dynamodb(id, data):
    response = MovieTable.put_item(
        Item = {
            'id'     : id,
            'data'  : data,
            
        }
    )
    return response

def read_from_dynamodb(id):
    response = MovieTable.get_item(
        Key = {
            'id'     : id
        },
        AttributesToGet = [
            'data' # valid types dont throw error, 
        ]                      # Other types should be converted to python type before sending as json response
    )
    return response

    return response

def delete_from_movie(id):
    response = MovieTable.delete_item(
        Key = {
            'id': id
        }
    )

    return response
