## Module 3 Homework 

<b><u>Important Note:</b></u> <p> For this homework we will be using the 2022 Green Taxi Trip Record Parquet Files from the New York
City Taxi Data found here: </br> https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page </br>
If you are using orchestration such as Mage, Airflow or Prefect do not load the data into Big Query using the orchestrator.</br> 
Stop with loading the files into a bucket. </br></br>
<u>NOTE:</u> You will need to use the PARQUET option files when creating an External Table</br>

<b>SETUP:</b></br>
Create an external table using the Green Taxi Trip Records Data for 2022. </br>

```sql
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `project_id.taxis_dataset.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://taxis-bucket-448121-i4/taxi_parquet/green_tripdata_2022-*.parquet']
);
```

Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table). </br>
</p>

```sql
-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE taxis_dataset.green_tripdata_non_partitoned AS
SELECT * FROM taxis_dataset.external_green_tripdata;
```



## Question 1:
Question 1: What is count of records for the 2022 Green Taxi Data??
- 65,623,481
- **840,402 <-**
- 1,936,423
- 253,647

## Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br> 
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

```sql
-- For External Table
SELECT
  COUNT(DISTINCT PULocationID)
FROM
  taxis_dataset.external_green_tripdata

-- For Materialized  Table
SELECT
  COUNT(DISTINCT PULocationID) 
FROM
  taxis_dataset.green_tripdata_non_partitoned;
```

- **0 MB for the External Table and 6.41MB for the Materialized Table <-**
- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table
- 2.14 MB for the External Table and 0MB for the Materialized Table


## Question 3:
How many records have a fare_amount of 0?

```sql
SELECT
  COUNT(*) AS zero_fare_records
FROM
  taxis_dataset.green_tripdata_non_partitoned
WHERE
  fare_amount = 0;
```

- 12,488
- 128,219
- 112
- **1,622 <-**

## Question 4:
What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)

- Cluster on lpep_pickup_datetime Partition by PUlocationID
- **Partition by lpep_pickup_datetime  Cluster on PUlocationID <-**
- Partition by lpep_pickup_datetime and Partition by PUlocationID
- Cluster on by lpep_pickup_datetime and Cluster on PUlocationID

```sql
CREATE OR REPLACE TABLE taxis_dataset.green_tripdata_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM taxis_dataset.external_green_tripdata;
```

## Question 5:
Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime
06/01/2022 and 06/30/2022 (inclusive)</br>

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? </br>

Choose the answer which most closely matches.</br> 

```sql
-- materialized table
SELECT DISTINCT PULocationID FROM  taxis_dataset.green_tripdata_non_partitoned
WHERE lpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30';

-- clustered partitioned table
SELECT DISTINCT PULocationID FROM taxis_dataset.green_tripdata_partitoned_clustered
WHERE lpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30';
```

- 22.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- **12.82 MB for non-partitioned table and 1.12 MB for the partitioned table <-**
- 5.63 MB for non-partitioned table and 0 MB for the partitioned table
- 10.31 MB for non-partitioned table and 10.31 MB for the partitioned table


## Question 6: 
Where is the data stored in the External Table you created?

- Big Query
- **GCP Bucket<-**
- Big Table
- Container Registry


## Question 7:
It is best practice in Big Query to always cluster your data:
- True
- **False <-**

```
When is Clustering Not Necessary?
* Small Tables: For small tables, the performance gains from clustering may be negligible.
* Infrequent Queries: If the table is rarely queried, the overhead of maintaining clustered data may not be justified.
* Non-Filtered Queries: If your queries do not filter on specific columns, clustering may not provide any benefit.
* High Write Workloads: Clustering can increase the cost of write operations (e.g., INSERT, UPDATE, DELETE) because BigQuery needs to reorganize the data.
```


## (Bonus: Not worth points) Question 8:
No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

 
## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw3


