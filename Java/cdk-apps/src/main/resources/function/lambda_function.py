
import os
import json
import time
import boto3
from botocore.exceptions import ClientError
import base64
import logging
from logging import Logger
from pprint import pprint
from datetime import datetime

import avro.schema
from avro.io import DatumReader, BinaryDecoder

from confluent_kafka.avro import AvroConsumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.serialization import IntegerDeserializer, StringDeserializer
from confluent_kafka.schema_registry.avro import AvroDeserializer   # for Kafka value
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka.schema_registry import SchemaRegistryClient, RegisteredSchema
from confluent_kafka.schema_registry.error import SchemaRegistryError

LAMBDA_LOGGER:Logger=logging.getLogger()
IS_FIRST:bool=True

SCHEMA_REGISTRY_SECRET_NAME:str=None              # 'laap-sec-ue1-jarviskafkaconn-dev'
TOPIC_2_FIREHOSE_MAP:dict=dict()

kinesis_client = boto3.client('firehose')

AVRO_SCHEMA_REGISTRY_BASE_URL:str=None
AVRO_SCHEMA_REGISTRY_CLIENT:SchemaRegistryClient = None

UTF_8_DESERIALIZER:StringDeserializer = StringDeserializer('utf_8')
INT_DESERIALIZER:IntegerDeserializer = IntegerDeserializer()

INPUT_TOPIC:str = None
VALUE_SCHEMA:str = None
KEY_SERIALIZATION_CTX:SerializationContext = None
VALUE_SERIALIZATION_CTX:SerializationContext = None
AVRO_VALUE_DESERIALIZER:AvroDeserializer = None

KINESIS_FIREHOSE_NAME:str=None      # firehose name derived from topic name

def setup_lambda_logger(logger_name:str, lambda_event, lambda_context) -> None:

    LAMBDA_LOGGING_LEVEL_NAME = None
    try:
        ENV_LOGGING_LEVEL:str = os.getenv('LOGGING_LEVEL')
        if ENV_LOGGING_LEVEL:
            LAMBDA_LOGGING_LEVEL_NAME = ENV_LOGGING_LEVEL.upper()
            print(f'[INFO] Found ENV Variable "LOGGING_LEVEL". Using its value: {LAMBDA_LOGGING_LEVEL_NAME}')
        else:
            raise Exception('Could not find ENV Variable "LOGGING_LEVEL"!')
    except Exception as ex:
        print(f'[WARN] Could not find ENV Variable "LOGGING_LEVEL". Default to INFO')
        LAMBDA_LOGGING_LEVEL_NAME = logging.getLevelName(logging.INFO)

    # AWS Lambda quirkyness
    # rootLogger = logging.getLogger()
    # rootLogger.propagate = False  # remove default logger
    # for h in rootLogger.handlers:
    #   rootLogger.removeHandler(h)

    # DEBUG < INFO < WARNING < ERROR < CRITICAL
    ## TODO: add aws_request_id from Lambda Context
    # LOG_MSG_FORMAT = '[%(levelname)s]\t %(asctime)s %(message)s'
    # logging.basicConfig(format=LOG_MSG_FORMAT, level=LAMBDA_LOGGING_LEVEL_NAME)
    #lambda_logger = logging.getLogger(logger_name)

    global LAMBDA_LOGGER
    # LAMBDA_LOGGER = rootLogger
    
    # log_handler = logging.StreamHandler()
    # log_handler.setFormatter(logging.Formatter(LOG_MSG_FORMAT))
    # lambda_logger.addHandler(log_handler)

    LAMBDA_LOGGER.setLevel(LAMBDA_LOGGING_LEVEL_NAME)

    ENV_SCHEMA_REGISTRY_URL:str = os.getenv('SCHEMA_REGISTRY_URL')
    if ENV_SCHEMA_REGISTRY_URL:
        LAMBDA_LOGGER.debug(f'Found ENV Variable "SCHEMA_REGISTRY_URL". Value={ENV_SCHEMA_REGISTRY_URL}')

        global AVRO_SCHEMA_REGISTRY_BASE_URL
        AVRO_SCHEMA_REGISTRY_BASE_URL = ENV_SCHEMA_REGISTRY_URL
    else:
        raise Exception('Could not find ENV Variable "SCHEMA_REGISTRY_URL"!')

    ENV_SCHEMA_REGISTRY_SECRET_NAME:str = os.getenv('SCHEMA_REGISTRY_SECRET_NAME')
    if ENV_SCHEMA_REGISTRY_SECRET_NAME:
        LAMBDA_LOGGER.debug(f'Found ENV Variable "SCHEMA_REGISTRY_SECRET_NAME". Value={ENV_SCHEMA_REGISTRY_SECRET_NAME}')

        global SCHEMA_REGISTRY_SECRET_NAME
        SCHEMA_REGISTRY_SECRET_NAME = ENV_SCHEMA_REGISTRY_SECRET_NAME
    else:
        raise Exception('Could not find ENV Variable "SCHEMA_REGISTRY_SECRET_NAME"!')

    ENV_SSM_PARAM_NAME:str = os.getenv('SSM_PARAM_NAME')
    if (ENV_SSM_PARAM_NAME):
        LAMBDA_LOGGER.debug(f'Found ENV Variable "SSM_PARAM_NAME". Value={ENV_SSM_PARAM_NAME}')

        ssm_client = boto3.client('ssm')
        ssm_response = ssm_client.get_parameter(Name=ENV_SSM_PARAM_NAME)
        if (ssm_response):
            TOPIC_2_FIREHOSE_JSON:str = ssm_response['Parameter']['Value']
            LAMBDA_LOGGER.debug(f'SSM Parameter: {ENV_SSM_PARAM_NAME} Value={TOPIC_2_FIREHOSE_JSON}')

            global TOPIC_2_FIREHOSE_MAP
            TOPIC_2_FIREHOSE_MAP = json.loads(TOPIC_2_FIREHOSE_JSON)
        else:
            raise Exception(f'Could not localte/retrieve SSM Parameter: {ENV_SSM_PARAM_NAME}!')
    else:
        raise Exception('Could not find ENV Variable "SSM_PARAM_NAME"!')

    if LAMBDA_LOGGING_LEVEL_NAME == logging.getLevelName(logging.DEBUG):
        print('---Lamba Event---')
        pprint(lambda_event)
        print('---Lamba Event---')

        print('<===Lambda Context===>')
        print("[CONTEXT] Lambda function ARN:", lambda_context.invoked_function_arn)
        print("[CONTEXT] CloudWatch log stream name:", lambda_context.log_stream_name)
        print("[CONTEXT] CloudWatch log group name:",  lambda_context.log_group_name)
        print("[CONTEXT] Lambda Request ID:", lambda_context.aws_request_id)
        print("[CONTEXT] Lambda function memory limits in MB:", lambda_context.memory_limit_in_mb)
        # We have added a 1 second delay so you can see the time remaining in get_remaining_time_in_millis.
        time.sleep(1) 
        print("[CONTEXT] Lambda time remaining in MS:", lambda_context.get_remaining_time_in_millis())
        print('<===Lambda Context===>')

def get_secret(secret_name:str) -> dict:

    try:
        secrets_mgr_cli = boto3.client('secretsmanager')
        get_secret_value_response = secrets_mgr_cli.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        LAMBDA_LOGGER.error('ERROR %s', e)
        raise e
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return json.loads(s=secret)
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return json.loads(s=decoded_binary_secret)

def initialize(input_topic:str, isAuthenticated:bool=True) -> bool:
    global AVRO_SCHEMA_REGISTRY_BASE_URL

    SCHEMA_REGISTRY_CONF:dict=dict()
    if (isAuthenticated):
        secret_as_dict = get_secret(secret_name=SCHEMA_REGISTRY_SECRET_NAME)
        jarvis_usr:str = secret_as_dict['username']
        jarvis_pwd:str = secret_as_dict['password']
        assert (jarvis_usr != None and jarvis_pwd != None)

        JARVIS_AUTH_INFO:str=f'{jarvis_usr}:{jarvis_pwd}'
        SCHEMA_REGISTRY_CONF['basic.auth.user.info'] = JARVIS_AUTH_INFO
    else:
        AVRO_SCHEMA_REGISTRY_BASE_URL = 'http://dev-cdp-schema-registry-pvt.us-east-1.espndev.pvt:8081'

    SCHEMA_REGISTRY_CONF['url'] = AVRO_SCHEMA_REGISTRY_BASE_URL

    global AVRO_SCHEMA_REGISTRY_CLIENT
    AVRO_SCHEMA_REGISTRY_CLIENT = SchemaRegistryClient(conf=SCHEMA_REGISTRY_CONF)

    if input_topic:
        global INPUT_TOPIC
        INPUT_TOPIC = input_topic

        # dynamically construct name of the Firehose
        # e.g. lndcdcadsrtcrd_demographic -> laap-firehose-ue1-datasink-lndcdcadsrtcrd-demographic-sbx
        global TOPIC_2_FIREHOSE_MAP
        global KINESIS_FIREHOSE_NAME
        try:
            KINESIS_FIREHOSE_NAME = TOPIC_2_FIREHOSE_MAP[INPUT_TOPIC]
        except KeyError as topic_not_mapped_ex:
            LAMBDA_LOGGER.critical('[%s] Unmapped topic: %s in TOPIC_2_FIREHOSE_MAP: %s', input_topic, INPUT_TOPIC, TOPIC_2_FIREHOSE_MAP)
            raise Exception(f'Unmapped topic {INPUT_TOPIC} in TOPIC_2_FIREHOSE_MAP')

        global KEY_SERIALIZATION_CTX
        KEY_SERIALIZATION_CTX = SerializationContext(topic=input_topic, field=MessageField.KEY)

        global VALUE_SERIALIZATION_CTX
        VALUE_SERIALIZATION_CTX = SerializationContext(topic=input_topic, field=MessageField.VALUE)

        value_schema_name:str = f'{input_topic}-value'

        schema_registry_url:str = f'{AVRO_SCHEMA_REGISTRY_BASE_URL}/subjects/{value_schema_name}/versions/latest'
        LAMBDA_LOGGER.info('[%s] SCHEMA REGISTRY=%s', input_topic, schema_registry_url)

        try:
            found_schema:RegisteredSchema = AVRO_SCHEMA_REGISTRY_CLIENT.get_latest_version(subject_name=value_schema_name)
            LAMBDA_LOGGER.info('[%s] Found schema for %s [Version=%s] as Schema_Id: %s', input_topic, found_schema.subject, found_schema.version, found_schema.schema_id)

            global VALUE
            VALUE_SCHEMA = found_schema.schema.schema_str
            assert (None != VALUE_SCHEMA)
            LAMBDA_LOGGER.info(f'[{input_topic}] AVRO Schema for {value_schema_name} from {schema_registry_url}:\n---\n{VALUE_SCHEMA}\n---\n\n')

            global AVRO_VALUE_DESERIALIZER
            # AVRO_VALUE_DESERIALIZER = AvroDeserializer(schema_str=VALUE_SCHEMA, schema_registry_client=AVRO_SCHEMA_REGISTRY_CLIENT)
            AVRO_VALUE_DESERIALIZER = AvroDeserializer(schema_registry_client=AVRO_SCHEMA_REGISTRY_CLIENT)
        except SchemaRegistryError as schema_reg_err:
            LAMBDA_LOGGER.error('[%s] ERROR %s', input_topic, schema_reg_err)
            return False

        return True
    else:
        raise RuntimeError('Need to provided valid topic name')

def write_to_firehose(topic:str, records:list) -> int:
    now_utc:datetime = datetime.utcnow()
    epoch = now_utc.timestamp()

    total_records_count:int = len(records)

    try:
        LAMBDA_LOGGER.info('[%s] Invoking batch PUT to Firehose %s', topic, KINESIS_FIREHOSE_NAME)

        # for avro_val in records:
        #     json_str:str = json.dumps(avro_val)  # TRYOUT: should append delimiter? + '\n'
        #     # b64_avro_val = base64.b64encode(json_str)
        #     LAMBDA_LOGGER.debug('[%s] PUT request= %s', topic, response)
        #     bytes_avro_val = json_str.encode()
        #     response = kinesis_client.put_record(
        #         DeliveryStreamName=KINESIS_FIREHOSE_NAME,
        #         Record={
        #             'Data': bytes_avro_val #'Data': b64_avro_val
        #         }
        #     )
        #     LAMBDA_LOGGER.debug('[%s] PUT response= %s', topic, response)

        # firehose_records:list = [ {'Data': json.dumps(avro_val).encode() for avro_val in records } ]
        firehose_records:list = [ {'Data': json.dumps(avro_val).encode()} for avro_val in records ]
        LAMBDA_LOGGER.info('[%s] PUT batch, REQUEST.ORIG count=[%s]', topic, total_records_count)
        LAMBDA_LOGGER.debug('[%s] PUT batch, REQUEST.ORIG=%s', topic, firehose_records)
        batch_response = kinesis_client.put_record_batch(
            DeliveryStreamName=KINESIS_FIREHOSE_NAME,
            Records=firehose_records
        )
        failed_put_count:int = batch_response['FailedPutCount']
        if (failed_put_count > 0):
            LAMBDA_LOGGER.warn('[%s] PUT batch, RESPONSE.FAILED count=[%s]', topic, failed_put_count)
            LAMBDA_LOGGER.warn('[%s] PUT batch, RESPONSE.FAILED=%s', topic, batch_response)

        successful_put_record_count:int = (total_records_count - failed_put_count)
        LAMBDA_LOGGER.info('[%s] PUT batch, RESPONSE.EFFECTIVE count=[%s]', topic, successful_put_record_count)
        return successful_put_record_count
    except Exception as ex:
        print(f'ERROR= {ex}')
        LAMBDA_LOGGER.error(ex)
        return None


def decode_key(encoded_key:str):
    decoded_key_bytes:bytes = base64.b64decode(encoded_key)
    # LAMBDA_LOGGER.debug('Encoded Key: %s', encoded_key)
    # LAMBDA_LOGGER.debug('Decoded Key: %s', decoded_key_bytes)

    try:
        LAMBDA_LOGGER.debug('1. Trying with String Deserializer')
        return UTF_8_DESERIALIZER(value=decoded_key_bytes, ctx=KEY_SERIALIZATION_CTX)
    except Exception as e1:
        LAMBDA_LOGGER.error('ERROR %s', e1)
        try:
            LAMBDA_LOGGER.debug('2. Trying with Integer Deserializer')
            return INT_DESERIALIZER(value=decoded_key_bytes, ctx=KEY_SERIALIZATION_CTX)
        except Exception as e2:
            LAMBDA_LOGGER.error('ERROR %s', e2)
            LAMBDA_LOGGER.debug('3. Trying with simple ASCII decode')
            return decoded_key_bytes.decode('ascii')

def decode_avro(schema:str, encoded_value:str) -> dict:
    decoded_value_bytes:bytes = base64.b64decode(encoded_value)
    # LAMBDA_LOGGER.debug('Encoded Value: %s', encoded_value)
    # LAMBDA_LOGGER.debug('Decoded Value: %s', decoded_value_bytes)

    try:
        global AVRO_VALUE_DESERIALIZER
        return_values = AVRO_VALUE_DESERIALIZER(decoded_value_bytes, ctx=VALUE_SERIALIZATION_CTX)
        return return_values
    except Exception as ex:
        LAMBDA_LOGGER.error("[%s] ERROR while decoding AVRO", schema)
        LAMBDA_LOGGER.error(ex)
        global VALUE_SCHEMA
        avro_schema = avro.schema.Parse(VALUE_SCHEMA)
        avro_reader = DatumReader(avro_schema)
        decoded_value_bytes.seek(5)
        binary_decoder = BinaryDecoder(decoded_value_bytes)
        return avro_reader.read(binary_decoder)

def lambda_handler (event, context):
    global IS_FIRST
    global LAMBDA_LOGGER

    # Collect basic information to construct test input message
    if IS_FIRST:
        lambda_name:str = context.function_name

        setup_lambda_logger(logger_name=lambda_name, lambda_event=event, lambda_context=context)
        print('Configured {lambda_name} with log level {log_level}'.format(lambda_name=lambda_name, log_level=LAMBDA_LOGGER.getEffectiveLevel()))

        IS_FIRST = False

    avro_records:list = []
    is_initialized:bool = False

    event_records = event['records']
    for kafka_partition in event_records:
        LAMBDA_LOGGER.info('---')
        kafka_records = event['records'][kafka_partition]
        records_cnt:int = len(kafka_records)
        LAMBDA_LOGGER.info('Total %d records for partition: %s', records_cnt, kafka_partition)
        for krecord in kafka_records:
            topic:str = krecord['topic']
            if (not is_initialized):
                is_initialized = initialize(input_topic=topic, isAuthenticated=True)

            # proceed only if initized
            assert (True == is_initialized)

            # krecord_key:str = krecord['key']
            # decrypted_key = decode_key(encoded_key=krecord_key)
            decrypted_key='KAFKA_KEY_NOT_USED'

            krecord_val:str = krecord['value']

            ### TEST without decoding AVRO
            decrypted_val = decode_avro(schema=topic, encoded_value=krecord_val)
            #decrypted_val = krecord_val

            # LAMBDA_LOGGER.info('Orig Key=%s & Orig Value=%s', krecord_key, krecord_val)
            LAMBDA_LOGGER.debug('%s===%s', decrypted_key, decrypted_val)

            avro_records.append(decrypted_val)

    # send to KDF
    if is_initialized:
        firehose_response = write_to_firehose(topic=INPUT_TOPIC, records=avro_records)

        if (firehose_response):
            total_success_cnt:int = firehose_response
            return_dict:dict = {"firehose": KINESIS_FIREHOSE_NAME , "total_records": total_success_cnt}
            return {
                'statusCode': 200,
                'body': return_dict
            }
        else:
            return {
                'statusCode': 500,
                'body': f'Error sending to Kinesis Firehose: {KINESIS_FIREHOSE_NAME}'
            }
    else:
        raise RuntimeError('Encountered uninitialized state')
