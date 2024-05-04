from airflow import DAG 
from airflow.providers.amazon.aws.transform.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.operators.dagrun_operator import TriggerDagRunOperator

from datetime import datetime, timedelta

from airflow.utils.dates import days_ago


default_agrs = {
                    'owner': 'airflow',
                    'depends_on_past' : False,
                    'email_on_failure': False,
                    'email_on_retry': False,
                    'retries': 1,
                    'retry_delay': timedelta(minutes=5),

}


with DAG (
            'create_and_load_dim',
            default_agrs =default_agrs,
            description = 'ETL for food delivery data into redshift',
            schedule_interval =None,
            start_date =days_ago(1),
            catchup =False
) as dag:
    

    create_schema = PostgresOperator(
                task_id ='create_schema',
                postgres_conn_id ='redshift_connection_id',
                sql = 'CREATE SCHEMA IF NOT EXISTS food_delivery_datamart;',

    )


    drop_dimCustomers = PostgresOperator(
            task_id = 'drop_dimCustomers',
            postgres_conn_id ='redshift_connection_id',
            sql = 'DROP TABLE IF EXISTS food_delivery_datamart.dimCustomers;',

    )

    
    drop_dimRestaurants = PostgresOperator(
            task_id = 'drop_dimRestaurants',
            postgres_conn_id ='redshift_connection_id',
            sql = 'DROP TABLE IF EXISTS food_delivery_datamart.dimRestaurants;',

    )

    
    drop_dimDeliveryRiders = PostgresOperator(
            task_id = 'drop_dimDeliveryRiders',
            postgres_conn_id ='redshift_connection_id',
            sql = 'DROP TABLE IF EXISTS food_delivery_datamart.dimDeliveryRiders;',

    )

    
    drop_factOrders = PostgresOperator(
            task_id = 'drop_factOrders',
            postgres_conn_id ='redshift_connection_id',
            sql = 'DROP TABLE IF EXISTS food_delivery_datamart.factOrders;',

    )


create_dimCustomers = PostgresOperator (
                        tast_id = 'create_dimCustomers',
                        postgres_conn_id = 'redshift_connection_id',
                        sql = """
                                create table if not exists food_delivery_datamart.dimCustomers 
                                (
                                        CustomerID INT Primary key,
                                        CustomerName varchar(255),
                                        CustomerEmail varchar(255),
                                        CustomerPhone varchar(50),
                                        CustomerAddress varchar(500),
                                        RegistrationDate Date
                                );                              
                            """,
                        )



create_dimRestaurants = PostgresOperator (
                        tast_id = 'create_dimRestaurants',
                        postgres_conn_id = 'redshift_connection_id',
                        sql = """
                                create table if not exists food_delivery_datamart.dimRestaurants 
                                (
                                        RestaurantID INT Primary key,
                                        RestaurantName varchar(255),
                                        CuisineType varchar(100),
                                        RestaurantAddress varchar(500),
                                        RestaurantRating Decimal(3,1)
                                        
                                );                              
                            """,
                        )



create_dimDeliveryRiders = PostgresOperator (
                        tast_id = 'create_dimDeliveryRiders',
                        postgres_conn_id = 'redshift_connection_id',
                        sql = """
                                create table if not exists food_delivery_datamart.dimDeliveryRiders 
                                (
                                        RiderID INT Primary key,
                                        RiderName varchar(255),
                                        RiderPhone varchar(50),
                                        RiderVehicleType varchar(50),
                                        VehicleID varchar(50),
                                        RiderRating Decimal(3,1)
                                );                              
                            """,
                        )


create_factOrders = PostgresOperator (
                        tast_id = 'create_factOrders',
                        postgres_conn_id = 'redshift_connection_id',
                        sql = """
                                create table if not exists food_delivery_datamart.factOrders 
                                (
                                        OrderID INT Primary key,
                                        CustomerID INT References food_delivery_datamart.dimCustomers(CustomerID),
                                        RestaurantID INT References foor_delivery_datamary.dimRestaurants(RestaurantID),
                                        RiderID INT References foor_delivery_datamary.dimDeliveryRiders(RiderID),
                                        OrderDate TIMESTAMP WITHOUT TIME ZONE,
                                        DeliveryTime INT,
                                        OrderValue Decimal(8,2),
                                        DeliveryFee decimal(8,2),
                                        TipAmount decimal(8,2),
                                        OrderStatus varchar(50)
                                );                              
                            """,
                        )


load_dimCustomers = S3ToRedshiftOperator(
                        task_id = 'load_dimCustomers',
                        schema = 'food_delivery_datamart',
                        table = 'dimCustomers',
                        s3_bucket = 'food-delivery-data-analysis',
                        s3_key ='dims/dimCustomers.csv',
                        copy_options  = ['CSV','IGNOREHEADER 1','QUOTE as \' "\''],
                        aws_conn_id = 'aws_default',
                        redshift_conn_id = 'redshift_connection_id',

)


load_dimRestaurants = S3ToRedshiftOperator(
                        task_id = 'load_dimRestaurants',
                        schema = 'food_delivery_datamart',
                        table = 'dimRestaurants',
                        s3_bucket = 'food-delivery-data-analysis',
                        s3_key ='dims/dimRestaurants.csv',
                        copy_options  = ['CSV','IGNOREHEADER 1','QUOTE as \' "\''],
                        aws_conn_id = 'aws_default',
                        redshift_conn_id = 'redshift_connection_id',

)

load_dimDeliveryRiders = S3ToRedshiftOperator(
                        task_id = 'load_dimDeliveryRiders',
                        schema = 'food_delivery_datamart',
                        table = 'dimDeliveryRiders',
                        s3_bucket = 'food-delivery-data-analysis',
                        s3_key ='dims/dimDeliveryRiders.csv',
                        copy_options  = ['CSV','IGNOREHEADER 1','QUOTE as \' "\''],
                        aws_conn_id = 'aws_default',
                        redshift_conn_id = 'redshift_connection_id',

)


trigger_spark_streaming_dag = TriggerDagRunOperator(
                                task_id = "trigger_spark_streaming_dag",
                                trigger_dag_id ="submit_pyspark_streaming_job_to_emr",
)


create_schema >> [drop_dimCustomers, drop_dimRestaurants, drop_dimDeliveryRiders,drop_factOrders]

drop_dimCustomers >> create_dimCustomers
drop_dimRestaurants >> create_dimRestaurants
drop_dimDeliveryRiders >> create_dimDeliveryRiders
drop_factOrders >> create_factOrders

[create_dimCustomers,create_dimRestaurants,create_dimDeliveryRiders] >> create_factOrders

create_dimCustomers>> load_dimCustomers
create_dimRestaurants >> load_dimRestaurants
create_dimDeliveryRiders >> load_dimDeliveryRiders

[load_dimCustomers,load_dimRestaurants,load_dimDeliveryRiders] >> trigger_spark_streaming_dag
