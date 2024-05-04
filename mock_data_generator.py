import pandas as pd
import json
import random
from datetime import datetime
from faker import Faker
import boto3
import os
import time 


fake  = Faker()
kinesis_client =boto3.client('kinesis')


base_directory = os.path.dirname(__file__)
data_directory = os.path.join(base_directory, "data_for_dims")

def load_ids_from_csv(file_name, key_attribute):
    file_path =os.path.join(data_directory,file_name)
    df = pd.read_csv(file_path)
    return df[key_attribute].tolist()


def generate_order(customer_ids, restaurant_ids, rider_ids, order_id):
    order = {
        'OrderID' : order_id,
        'CustomerID' : random.choice(customer_ids),
        'RestaurantID' : random.choice(restaurant_ids),
        'RiderID': random.choice(rider_ids),
        'OrderDate': fake.date_time_between(start_date = '-30d',end_date = 'now').isoformat(),
        'DeliveryTime': random.randint(15,60),
        'OrderValue': round(random.uniform(10,100),2),
        'DeliveryFee': round(random.uniform(2,10),2),
        'TipAmount': round(random.uniform(0,20),2),
        'OrderStatus' : random.choice(['Delivered', 'Cancelled', 'Processing', 'On the way'])
    }
    return order


def send_order_to_kinesis(stream_name, order):
    response = kinesis_client.put_record(
        StreamName = stream_name,
        Data = json.dumps(order),
        PartitionKey = str(order['OrderID'])
    )
    print(f"Sent order to Kinesis with Sequence Number: {response['SequenceNumber']}")




customer_ids = load_ids_from_csv('dimCustomers.csv','CustomerID')
restaurant_ids = load_ids_from_csv('dimRestaurants.csv','dimRestaurants')
rider_ids = load_ids_from_csv('dimDeliveryRiders.csv','RiderID')


stream_name = 'incoming-food-order-data'

order_id = 5000

for _ in range(1000):
    order = generate_order(customer_ids, restaurant_ids, rider_ids, order_id)
    print(order)
    send_order_to_kinesis(stream_name,order)
    order_id +=1









