from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, StructField, StructType, DecimalType, TimestampType
from pyspark.sql.functions import col,from_json
import argparse


parser = argparse.ArgumentParser(description= 'PySpark Streaming Job Arguments')
parser.add_argument('--redshift_user', required= True, help= 'Redshift Username')
parser.add_argument('--redshift_password', required= True, help= 'Redshift Password')
parser.add_argument('--aws_access_key', required= True, help= 'aws_access_key')
parser.add_argument('--aws_secret_key', required= True, help= 'aws_secret_key')
args = parser.parse_args()


appName = "KinesisToRedshift"
kinesisStreamName = 'incoming-food-order-data'
kinesisRegion = "us-east-1"
checkpointLocation = 's3://stream-checkpointing/kinesisToRedshift/'
redshiftJdbcUrl = 
redshiftTable = 
tempDir = 

schema = StructType([
                StructField('OrderID',IntegerType(),True),
                StructField('CustomerID',IntegerType(),True),
                StructField('RestaurantID',IntegerType(),True),
                StructField('RiderID',IntegerType(),True),
                StructField('OrderDate',TimestampType(),True),
                StructField('DeliveryTime',IntegerType(),True),
                StructField('OrderValue',DecimalType(),True),
                StructField('DeliveryFee',DecimalType(),True),
                StructField('TipAmount',DecimalType(),True),
                StructField('OrderStatus',StringType(),True)
])


spark = SparkSession.builder.appName(appName).getOrCreate()


df =spark.readStream \
         .format('kinesis') \
         .option('streamName',kinesisStreamName) \
         .option('startingPosition','lastest') \
         .option('region',kinesisRegion) \
         .option('awsUseInstanceProfile','fasle') \
         .option('endpointUrl','https://kinesis.us-east-1.amazonaws.com') \
         .option('awsAccessKeyId',args.aws_access_key) \
         .option('awsSecretKey', args.aws_secret_key) \
         .load()

print("consuming from read stream........")

parsed_df = df.selectExpr("CAST(data as string)").select(from_json(col('data'),schema).alias("parsed_data")).select('parsed_data.*')

deduped_df = parsed_df.withWatermark("OrderDate","10 minutes").dropDuplicates(['OrderID'])


def write_to_redshift(batch_df,batch_id):
    batch_df.write \
            .format('jdbc') \
            .option('url',redshiftJdbcUrl) \
            .option('user',args.redshift_user) \
            .option('password',args.redshift_password) \
            .option('dbtable', redshiftTable) \
            .option('tempdir', tempDir) \
            .option('driver','com.amazon.redshift.jdbc.Driver') \
            .mode('append') \
            .save()
    


query = deduped_df.writeStream \
                  .foreachBatch(write_to_redshift) \
                  .outputMode('append') \
                  .trigger(processingTime = '5 seconds') \
                  .option("checkpointLocation",checkpointLocation) \
                  .start()


print("Current batch written in redshift")

query.awaitTermination()