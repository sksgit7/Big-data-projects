from flask import Flask, render_template, request, url_for
import requests
import datetime, time
import random
import config
import mysql.connector as conn

import argparse
from uuid import uuid4
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer

app = Flask(__name__)

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

def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))



@app.route("/", methods=['GET','POST'])
def bid():
	#Connecting to the DB and fetching the maximum bid till now
	cnx = conn.connect(host = "localhost", user = "root",
			passwd = "mysql", database = "test")
	cur = cnx.cursor()
	query = "select max(price) from bid;"

	cur.execute(query)
	result = cur.fetchone()[0]

	#if bid is submitted it will come as POST request.	
	if request.method == 'POST':
		#fetch the data from the web page
		name = request.form['name']
		price = request.form['price']
		bid_ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		#randomly generate a string which will be our unique key for each record or message
		msg_key = str(uuid4())

		#this code is there to randomly genearte name and price if we hit "Bid" without any values
		#this is to generate values quickly
		if name=='' or price=='':
			name = random.choice(['Aa','Bb','Cc','Dd','Ee','Ff','Gg','Hh','Ii','Jj'])
			price = round(random.random()*10000)
			response= {
				'name':name, 'price':int(price), 'bid_ts':bid_ts
			}
			print(msg_key,'Auto generated msg:',response)

		else:
			response= {
				'name':name, 'price':int(price), 'bid_ts':bid_ts
				}
			#print the key and the message on terminal window
			print(msg_key, ':', response)


		#---------------
		schema_registry_conf = schema_config()

		#inititalize the schema registry client to fetch the schema
		schema_registry_client = SchemaRegistryClient(schema_registry_conf)
		
		topic = 'auction'
		#getting the latest schema from Schema registry
		my_schema = schema_registry_client.get_latest_version(topic+'-value').schema.schema_str 
		#To serialize the keys
		string_serializer = StringSerializer('utf_8')
		#to serialize json data
		json_serializer = JSONSerializer(my_schema, schema_registry_client, to_dict=None)
		producer = Producer(sasl_conf())

		print("Producing user records to topic {}. ^C to exit.".format(topic))

		producer.poll(0.0)
		try:
			#produce message to Kafka topic
			producer.produce(topic=topic,
				#uuid4() generate random strings
				#everytime we produce a message it needs a key, if hardcode the key then message goes to same partition everytime
					key=string_serializer(msg_key, response),
					value=json_serializer(response, SerializationContext(topic, MessageField.VALUE)),
					on_delivery=delivery_report)

		except KeyboardInterrupt:
			pass
		except ValueError:
			print("Invalid input, discarding record...")
			pass


		print("\nFlushing records...")
		producer.flush()

		#if the current price that bidder sent is more than maximum price then it becomes the maximum bid
		if int(price) > result:
			result=int(price)

		#return the template index.html on browser with mentioned arguments
		return render_template('index.html', bid_added=True, highest_bid=result)
	#if method=GET (when we just reload the page or visit for first time)
	return render_template('index.html', bid_added=False, highest_bid=result)


if __name__ == '__main__':
	app.run(debug=True)