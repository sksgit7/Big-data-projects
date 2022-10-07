import argparse
import datetime
import csv
import config

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient


API_KEY, ENDPOINT_SCHEMA_URL, API_SECRET_KEY, BOOTSTRAP_SERVER, SECURITY_PROTOCOL, SSL_MECHANISM, SCHEMA_REGISTRY_API_KEY, SCHEMA_REGISTRY_API_SECRET = config.config_values()


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MECHANISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }


def main(topic):


    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    my_schema = schema_registry_client.get_latest_version(topic+'-value').schema.schema_str 

    json_deserializer = JSONDeserializer(my_schema,
                                         from_dict=None)

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'group2',
                     'auto.offset.reset': "earliest"})     #or earliest, latest

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    counter=0
    with open('./output.csv', 'a+', newline='') as f:
        w = csv.writer(f)
        #go to the starting of the file
        f.seek(0)
        #if file is new (empty) just add the header record, we can get this header list from schema too
        if len(f.readlines())==0:
            f.seek(0)
            w.writerow(['name','price','bid_ts'])
        while True:
            try:
                # SIGINT can't be handled when polling, limit timeout to 1 second.
                msg = consumer.poll(1.0)
                if msg is None:
                    continue

                bid = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                print('bid:',bid)

                if bid is not None:
                    counter+=1
                    #print(datetime.datetime.now())
                    print("User record {}: bid: {}"
                          .format(msg.key(), bid))

                    #print(type(order.record))
                    #print(order.record.values())

                    #create a list with column values of individual rows and insert to the file
                    rowList = []
                    for col in bid.values():
                        rowList.append(col)

                    w.writerow(rowList)

                    print('Total messages fetched till now:', counter)
                    
            except KeyboardInterrupt:
                break

    consumer.close()

main("auction")