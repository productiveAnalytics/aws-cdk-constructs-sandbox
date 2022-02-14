import os
import json
import logging
import base64
from pprint import pprint
from datetime import datetime

import avro.schema
from avro.io import DatumReader, BinaryDecoder

from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient, RegisteredSchema
from confluent_kafka.schema_registry.error import SchemaRegistryError

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
LOGGER = logging.getLogger('laap-lambda-avro-2-glue-table')

SECRET_NAME:str=None                # 'laap-sec-ue1-jarviskafkaconn-dev'
OUTPUT_S3_PATH:str=None

ENV_OUTPUT_S3_FOLDER:str = os.getenv('OUTPUT_S3_FOLDER')
if (ENV_OUTPUT_S3_FOLDER):
    LOGGER.info(f'Found ENV Variable "ENV_OUTPUT_S3_FOLDER". Value={ENV_OUTPUT_S3_FOLDER}')
    OUTPUT_S3_PATH = ENV_OUTPUT_S3_FOLDER
else:
    raise Exception('Could not fined ENV Variable: OUTPUT_S3_FOLDER.')
   
ENV_SECRET_NAME:str = os.getenv('SECRET_NAME')
if ENV_SECRET_NAME:
    LOGGER.info(f'Found ENV Variable "SECRET_NAME". Value={ENV_SECRET_NAME}')
    SECRET_NAME = ENV_SECRET_NAME
else:
   raise Exception('Could not fined ENV Variable: SECRET_NAME.')

AVRO_SCHEMA_REGISTRY_BASE_URL:str=None
AVRO_SCHEMA_REGISTRY_CLIENT:SchemaRegistryClient = None

VALUE_SCHEMA:str = None

def get_secret(secret_name:str) -> dict:
    import boto3
    from botocore import ClientError
    try:
        secrets_mgr_cli = boto3.client('secretsmanager')
        get_secret_value_response = secrets_mgr_cli.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        LOGGER.error('ERROR %s', e)
        raise e
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return json.loads(s=secret)
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return json.loads(s=decoded_binary_secret)


def initialize(isAuthenticated:bool=True) -> bool:
    global AVRO_SCHEMA_REGISTRY_BASE_URL
    
    SCHEMA_REGISTRY_CONF:dict=dict()
    if (isAuthenticated):
        AVRO_SCHEMA_REGISTRY_BASE_URL = 'https://us-east-1-adsales-sb-main.data-integration.dtcisb.technology:8080'
        secret_as_dict = get_secret(secret_name=SECRET_NAME)
        jarvis_usr:str = secret_as_dict['schemaRegistryUsername']
        jarvis_pwd:str = secret_as_dict['schemaRegistryPassword']
        assert (jarvis_usr != None and jarvis_pwd != None)
        
        JARVIS_AUTH_INFO:str=f'{jarvis_usr}:{jarvis_pwd}'
        SCHEMA_REGISTRY_CONF['basic.auth.user.info'] = JARVIS_AUTH_INFO
    else:
        AVRO_SCHEMA_REGISTRY_BASE_URL = 'http://dev-cdp-schema-registry-pvt.us-east-1.espndev.pvt:8081'

    SCHEMA_REGISTRY_CONF['url'] = AVRO_SCHEMA_REGISTRY_BASE_URL

    #print('SCHEMA_REGISTRY_CONF=')
    #pprint(SCHEMA_REGISTRY_CONF)

    global AVRO_SCHEMA_REGISTRY_CLIENT
    AVRO_SCHEMA_REGISTRY_CLIENT = SchemaRegistryClient(conf=SCHEMA_REGISTRY_CONF)


def get_glue_schema(input_topic:str) -> str:
    if input_topic:
        global INPUT_TOPIC
        INPUT_TOPIC = input_topic
        
        value_schema_name:str = f'{input_topic}-value'

        schema_registry_url:str = f'{AVRO_SCHEMA_REGISTRY_BASE_URL}/subjects/{value_schema_name}/versions/latest'
        LOGGER.info('SCHEMA REGISTRY=%s', schema_registry_url)
    
        try:
            found_schema:RegisteredSchema = AVRO_SCHEMA_REGISTRY_CLIENT.get_latest_version(subject_name=value_schema_name)
            LOGGER.info('Found schema for %s [Version=%s] as Schema_Id: %s', found_schema.subject, found_schema.version, found_schema.schema_id)
            
            global VALUE
            VALUE_SCHEMA = found_schema.schema.schema_str
            assert (None != VALUE_SCHEMA)
            print(f'AVRO Schema for {value_schema_name} from {schema_registry_url}:\n---\n{VALUE_SCHEMA}\n---\n\n')
            
            
        except SchemaRegistryError as schema_reg_err:
            LOGGER.error('ERROR %s', schema_reg_err)
            return False
            
        return True
    else:
        raise RuntimeError('Need to provided valid topic name')


def lambda_handler (event, context):
    LOGGER.info('Context: %s', context)
    pprint(context)

    print(f'Event: {event}')
    LOGGER.info('Event: %s', event)
    pprint(event)
    event_records = event['records']
    LOGGER.info('Total records %d', len(event_records))
    
    s3_details_as_str:str = event_records[0]['s3']
    s3_details_as_dict:dict = json.loads(s3_details_as_str)
    pprint(s3_details_as_dict)
    bucket_name:str = s3_details_as_dict['bucket']['name']
    object_key:str = s3_details_as_dict['object']['key']
    
    input_s3_path:str = f'{bucket_name}/{object_key}'
    LOGGER.info('Input file: %s', input_s3_path)
    
    input_tables:list = read_input(input_file_path=input_s3_path)
    success_avro_2_glue_schema:list = list()
    
    diff_cnt:int = len(success_avro_2_glue_schema) - len(input_tables)

    if (diff_cnt == 0):
        # All successful
        qualified_output_s3_folder:str = 'todo_build_s3_output_folder_path'
        return {
            'statusCode': 200,
            'body': f'All tables converted to Glue schemas under: {qualified_output_s3_folder}'
        }
    elif (diff == len(input_tables)):
        # All errored
        return {
            'statusCode': 500,
            'body': f'Error processing S3 event'
        }
    else:
        # partial success
        return {
            'statusCode': 206,
            'body': f'{diff_cnt} tables were unable to convert from AVRO to Glue schema(s)'
        }