from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.types import StringType
import os
from cryptography.fernet import Fernet

# Function to start SparkSession
def get_spark_session():
    return SparkSession.builder \
        .appName("Logistik Processing") \
        .config("spark.jars", "/home/hadoop/postgresql-42.2.27.jar") \
        .config("spark.driver.extraClassPath", "/home/hadoop/postgresql-42.2.27.jar") \
        .getOrCreate()

# UDF for masking nama_pelanggan
def mask_name_udf():
    def mask_name(name):
        if name:
            parts = name.split()
            masked_parts = [
                part[0] + part[1:3] + '*' * (len(part) - 3) if len(part) > 3 else part[0] + '*' * (len(part) - 1)
                for part in parts
            ]
            return ' '.join(masked_parts)
        return None
    return mask_name

# Task 1: Extract Data
def extract_data(**kwargs):
    spark = get_spark_session()
    jdbc_url = "jdbc:postgresql://127.0.0.1:5432/postgres"
    jdbc_properties = {"user": "postgres", "password": "rian", "driver": "org.postgresql.Driver"}

    # Extract data from database
    data = spark.read.jdbc(url=jdbc_url, table="logistik", properties=jdbc_properties)
    data.write.parquet("/home/hadoop/airflow/dags/temp_logistik", mode="overwrite")
    print("Data successfully extracted and saved temporarily.")
    spark.stop()

# Task 2: Mask Data
def mask_data(**kwargs):
    spark = get_spark_session()
    mask_name = mask_name_udf()
    spark.udf.register("mask_name", mask_name, StringType())

    # Load data
    data = spark.read.parquet("/home/hadoop/airflow/dags/temp_logistik")

    # Mask columns
    data_masked = data.withColumn(
        "id_pelanggan", expr("concat(substring(id_pelanggan, 1, 3), repeat('*', length(id_pelanggan) - 3))")
    ).withColumn(
        "nama_pelanggan", expr("mask_name(nama_pelanggan)")
    ).withColumn(
        "lokasi_pelanggan", expr("concat(substring(lokasi_pelanggan, 1, 3), repeat('*', length(lokasi_pelanggan) - 3))")
    )

    data_masked.write.parquet("/home/hadoop/airflow/dags/temp_masked_logistik", mode="overwrite")
    print("Data successfully masked and saved temporarily.")
    spark.stop()

# Task 3: Save Masked Data to pentaho_db
def save_to_pentaho_db(**kwargs):
    spark = get_spark_session()
    jdbc_url_pentaho = "jdbc:postgresql://127.0.0.1:5432/pentaho_db"
    jdbc_properties = {"user": "postgres", "password": "rian", "driver": "org.postgresql.Driver"}

    # Load masked data
    data_masked = spark.read.parquet("/home/hadoop/airflow/dags/temp_masked_logistik")

    # Save to pentaho_db
    data_masked.write.jdbc(
        url=jdbc_url_pentaho,
        table="masked_logistik",
        mode="overwrite",
        properties=jdbc_properties
    )
    print("Masked data successfully saved to pentaho_db.")
    spark.stop()

# Task 4: Encrypt Data
def encrypt_data(**kwargs):
    input_path = "/home/hadoop/airflow/dags/temp_masked_logistik"
    encrypted_file = "/home/hadoop/airflow/dags/logistik_encrypted_file.csv"
    key_file = "/home/hadoop/airflow/dags/encryption_key.key"

    # Generate encryption key
    key = Fernet.generate_key()
    cipher = Fernet(key)

    # Save key
    with open(key_file, "wb") as file:
        file.write(key)

    # Read and encrypt data
    spark = get_spark_session()
    data_masked = spark.read.parquet(input_path)
    pandas_df = data_masked.toPandas()
    csv_data = pandas_df.to_csv(index=False).encode()

    encrypted_data = cipher.encrypt(csv_data)

    with open(encrypted_file, "wb") as file:
        file.write(encrypted_data)

    print(f"Data encrypted and saved to {encrypted_file}")
    print(f"Encryption key saved to {key_file}")
    spark.stop()

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['beorian99@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'logistik_proses',
    default_args=default_args,
    description='DAG for masking and encrypting logistik data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 12, 1),
    catchup=False,
) as dag:

    extract_data_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    mask_data_task = PythonOperator(
        task_id='mask_data',
        python_callable=mask_data
    )

    save_to_pentaho_db_task = PythonOperator(
        task_id='save_to_pentaho_db',
        python_callable=save_to_pentaho_db
    )

    encrypt_data_task = PythonOperator(
        task_id='encrypt_data',
        python_callable=encrypt_data
    )

    # Set task dependencies
    extract_data_task >> mask_data_task >> save_to_pentaho_db_task >> encrypt_data_task
