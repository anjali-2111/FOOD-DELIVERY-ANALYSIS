from airflow import DAG
from airflow.providers.amazon.aws.operator.emr  import EmrAddStepsOperator
from airflow.models import Variables
from datetime import datetime

dag = DAG(
            'submit_pyspark_streaming_job_to_emr',
            start_date = datetime(2021,1,1),
            catchup =False,
            tags = ['streaming'],
)


spark_packages = [
         "com.qubole.spark:spark-sql-kinesis_2.12:1.2.0_spark-3.0",
         "io.github.spark-redshift-community:spark-redshift_2.12:6.2.0-spark_3.5"
]


packages_list = ','.join(spark_packages)

jdbc_jar_s3_path = "s3://food-delivery-data-analysis/redshift-connector-jar/redshift-jdbc42-2.1.0.12.jar"

redshift_user = Variables.get("redshift_user")
redshift_password = Variables.get("redshift_password")
aws_access_key = Variables.get("aws_access_key")
aws_secret_key = Variables.get("aws_secret_key")

step_adder = EmrAddStepsOperator(
                task_id = 'add_step',
                job_flow_id = 'j-34WHJYRAKN8XF',
                aws_conn_id = 'aws_default',
                steps = [{
                    'Name': 'Run Pyspark streaming script',
                    'ActionOnFailure' : 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args' : [
                                    'spark-submit',
                                    '--deploy-mode',
                                    'cluster',
                                    '--num-executors','3',
                                    '--executor-memory','6G',
                                    '--executor-cores','3',
                                    '--packages',packages_list,
                                    '--jars',jdbc_jar_s3_path,
                                    's3://food-delivery-data-analysis/pyspark_script/pyspark_streaming.py',
                                    '--redshift_user',redshift_user,
                                    '--redshift_password',redshift_password,
                                    '--aws_access_key',aws_access_key,
                                    '--aws_secret_key',aws_secret_key,
                        ],
                    },
                }],
                dag =dag,
)