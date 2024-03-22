from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
import requests
import json


# Write the API which is in JSON format and then opening it
with open('/home/ubuntu/airflow/config_api.json', 'r') as config_file:
    api_host_key = json.load(config_file)
    
# Loading the datetime.now() so all our data is stored uniquely
# Zillow API documentation link: https://rapidapi.com/s.mahmoud97/api/zillow56
now = datetime.now()
dt_now_string = now.strftime("%d%m%Y%H%M%S")

# Defining the S3 bucket which will be used in S3KeySensor
s3_bucket = "zillow-transform-from-json-to-csv"

def etl_zillow_data(**kwargs):
    url = kwargs['url']
    querystring = kwargs['querystring']
    headers = kwargs['headers']
    dt_string = kwargs['date_string']
    
    # return answer
    response = requests.get(url, headers=headers, params=querystring)
    response_data = response.json()
    
    # Specifying the output filepath only
    output_file_path = f"/home/ubuntu/response_data_{dt_string}.json"
    file_str = f"response_data_{dt_string}.csv"
    
    with open(output_file_path, "w") as output_file:
        json.dump(response_data, output_file, indent = 4)   # indent is only used for pretty looking
        
    # We are using this output_list and file_str to have CSV format in it and that will be used in S3 key sensor
    output_list = [output_file_path, file_str]
    
    return output_list 
    
    

# Default setting for a DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 8, 1),
    "email": ["prayagshah07@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=45),
}

with DAG('zillow_analytics_dag',
         default_args=default_args,
         schedule_interval = '@daily',
         catchup=False) as dag:
    
# Using this as a Python operator to call the zillow api script
        extract_zillow_data = PythonOperator(
        task_id="tsk_extract_zillow_data", 
        python_callable=etl_zillow_data,
        op_kwargs = {'url': 'https://zillow56.p.rapidapi.com/search', 'querystring': {"location": "houston, tx"}, 'headers': api_host_key, 'date_string': dt_now_string}
)

# xcom means cross communication. We are using [0] because we are geenrating list and from that we want have first part of the list output_file_path
        load_to_S3 = BashOperator(
            task_id="task_load_to_S3",
            bash_command = 'aws s3 mv {{ti.xcom_pull("tsk_extract_zillow_data")[0]}} s3://zillow-etl/',
        )

# We want to put a sensor once the data is transferred to CSV transformation bucket
# Important to know why bucket_key is chosen 1 because in the python function above we are having file_str in csv format
# and then calling output_list so [1] position become file_str which is in CSV
# aws_conn_id we need to create this connection in Airflow UI

        is_file_in_S3_available = S3KeySensor(
            task_id = "tsk_is_file_in_S3_available",
            bucket_key = '{{ti.xcom_pull("tsk_extract_zillow_data")[1]}}',
            bucket_name = s3_bucket,
            aws_conn_id='aws_s3_conn',
            wildcard_match=False,    # Set this True if you want to use wildcard in the prefix
            timeout=60,      # Timeout for the sensor
            poke_interval=5   #Time interval where S3 checks
        )
        
 # We will use the same aws conn id from the from the KeySensor
 # We will create the redshift conn id in Airflow in Admin 
 # Schema comes from redshift as it is public > zillowdata
 # copy_options as we want to ignore the first header of the csv file because we already have created the table in RedShift 
 # In connection ID there is host and that we will get it from RedShift endpoint on the page where cluster is created.     
        tsk_transfer_s3_to_redshift = S3ToRedshiftOperator(
            task_id = "tsk_transfer_s3_to_redshift",
            aws_conn_id = 'aws_s3_conn',
            redshift_conn_id = 'conn_id_redshift',
            s3_bucket=s3_bucket,
            s3_key = '{{ti.xcom_pull("tsk_extract_zillow_data")[1]}}',
            schema='PUBLIC',
            table='zillowdata',
            copy_options = ["csv IGNOREHEADER 1"]
        )
        
        
        extract_zillow_data >> load_to_S3 >> is_file_in_S3_available >> tsk_transfer_s3_to_redshift
