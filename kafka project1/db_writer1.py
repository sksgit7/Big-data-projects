import argparse
import datetime
import datetime, time
import mysql.connector as conn
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


#configurations for the Schema registry
def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }


def main(topic):

    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    #get the latest schema for the topic
    my_schema = schema_registry_client.get_latest_version(topic+'-value').schema.schema_str 

    json_deserializer = JSONDeserializer(my_schema,
                                         from_dict=None)

    consumer_conf = sasl_conf()
    #mention group id of this consumer application. It can be a random string too.
    #Consumers in same group share the messages in the topic.
    consumer_conf.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "earliest"})     #or earliest, latest

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    counter=0
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            #de-serialize the message
            bid = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if bid is not None:
                counter+=1
                print('Current timestamp:',datetime.datetime.now())
                print("User record {}: bid: {}"
                      .format(msg.key(), bid))
                print('Total messages fetched till now:', counter)

            name = bid['name']
            price = bid['price']
            bid_ts = bid['bid_ts']

            sql_ts = time.time()

            #Create DB connection and insert records
            try:
                #some process
                #time.sleep(3)
                ##
                cnx = conn.connect(host = "localhost", user = "root",
                    passwd = "your-password", database = "test")
                cur = cnx.cursor()
                query = "insert into bid (name, price, bid_ts) values ( %s, %s, %s)"
                data = (name,price,bid_ts)
    
                cur.execute(query,data)
    
                cnx.commit()
                print(cur.rowcount, " record is successfully added")
                cur.close()
                cnx.close()
                print('seconds spent to insert record:', time.time()-sql_ts)
                print('seconds spent from web page to table:', time.time()-time.mktime(time.strptime(bid_ts, '%Y-%m-%d %H:%M:%S')))
                print('-------------------------------')

            except conn.Error as err :
                if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                    print("Something is wrong with your user name or password")
                elif err.errno == errorcode.ER_BAD_DB_ERROR :
                    print("Database does not Exist")
                else :
                    print(err)
                err.error()
                
        except KeyboardInterrupt:
            break

    consumer.close()

main("auction")
