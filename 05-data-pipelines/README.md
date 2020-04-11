# Sparkify Data Pipeline

This is my assignment for the Data Pipeline Project Submission, which is a part of the Data Engineer Nanodegree.

The code contained in this folder defines an Apache Airflow ETL that extracts song and log data from the [Million Song Dataset](http://millionsongdataset.com/) that resides in S3 and writes the results to Redshift.

## Project background

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

The task was to build an ETL pipeline in Apache Airflow that
* Extracts song and log data from S3 and copies it to staging tables in Redshift
* Processes the data to create a fact table and dimension tables
* Performs simple data validation to determine whether the process was successful

## Files and folders
The following folders are used by Apache Airflow 
* `dags` folder contains
  * *sparkify_create_tables* DAG that creates the tables needed by the ETL process. You only need to run this once.
  * *sparkify_etl* DAG that runs the ETL process.
  * *get_staging_to_dim* SubDAG that loads data to dimensions tables and runs data validation.
* `plugins` folder contains
  * *SQLQueries* Class. All SQL queries used by the ETL reside here.
  * *StageToRedshiftOperator* that copies source data from S3 to the staging table.
  * *LoadFactOperator* that inserts data to the facts tables from staging.
  * *LoadDimensionOperator* that inserts data to the dimension tables from staging.
  * *DataQualityOperator* that performs data validation on the tables.
* `create_tables.sql` file contains SQL queries to create the tables used by the ETL process.

The remaining files and folders are needed when you run Airflow in a Docker environment
* `config` folder contains Airflow configuration file.
* `pgdata` folder is used to persist Airflow data
* `docker-compose.yml` file defines the Airflow environment in Docker
* `requirements.txt` defines additional libraries required by Airflow container to run

## Run Airflow in Docker environment (OPTIONAL)
You can spin up an Airflow environment in Docker in case you don't have a ready Ariflow environment to use.
* Install [Docker](https://www.docker.com/)
* Install [Docker Compose](https://docs.docker.com/compose/install/)
* Run `$ mkdir pgdata` in the root folder of this project to persist Airflow data such as connections, variables and history
* Run `$ docker-compose up` in the root folder of this project to start the environment
* Run `$ docker-compose down` in the root folder of this project to stop the environment

## Airflow Configuration

### Connections
* Add `aws_credentials` connection
  * Conn Type: Amazon Web Services
  * Login: `<YOUR AWS KEY>`
  * Password: `<YOUR AWS SECRET>`
* Add `redshift` connection
  * Conn Type: Postgres
  * Host: `<URL TO YOUR REDSHIFT CLUSTER>`
  * Schema: `<NAME OF YOUR DATABASE>`
  * Login: `<YOUR DB USERNAME>`
  * Password: `<YOUR DB PASSWORD>`
  * Port: 5439

### Variables
Create a key called `sparkify_conf` with val
```
{
"log_data":"s3://udacity-dend/log_data",
"log_format":"s3://udacity-dend/log_json_path.json",
"song_data":"s3://udacity-dend/song_data",
"truncate_dim_tables":1
}
```
We utilize JSON as a value in order to keep the variable amount small and also reduce the amount of database connections when reading variables in our ETL.

You can decide whether to truncate dimension tables upon ETL process by changing the value of `truncate_dim_tables`. *0* will append the new data, while *1* whill truncate and load the new data.

## Running the ETL process
We need to create the required tables before we can run the actual ETL process.

Enable the `sparkify_create_tables` DAG and Trigger execution once. After the ETL has successfully finished you can disable the DAG.

Enabled the `sparkify_etl` DAG once the tables have been created.


## Cleaning up
It is recommended to always delete the cluster when you're done testing as it might otherwise cause unexepted costs that you will have to pay. It is also be a good idea to remove any IAM role and EC2 key pair that you have used.
