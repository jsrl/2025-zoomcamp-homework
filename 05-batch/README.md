# Module 5 Homework

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the Yellow 2024-10 data from the official website: 

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet
```


## Question 1: Install Spark and PySpark

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?

> [!NOTE]
> To install PySpark follow this [guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/05-batch/setup/pyspark.md)

```sh
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
    
**'3.5.4'<-**
```
## Question 2: Yellow October 2024

Read the October 2024 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- 6MB  
- **25MB<-**
- 75MB
- 100MB

```sh
df = spark.read.parquet("yellow_tripdata_2024-10.parquet").repartition(4)
df.write.mode("overwrite").parquet("homework2025/")
!du -sh homework2025/*

0	homework2025/_SUCCESS
23M	homework2025/part-00000-12af7407-9f42-4d4d-9a9a-7052f7b6655c-c000.snappy.parquet
23M	homework2025/part-00001-12af7407-9f42-4d4d-9a9a-7052f7b6655c-c000.snappy.parquet
23M	homework2025/part-00002-12af7407-9f42-4d4d-9a9a-7052f7b6655c-c000.snappy.parquet
23M	homework2025/part-00003-12af7407-9f42-4d4d-9a9a-7052f7b6655c-c000.snappy.parquet
```
## Question 3: Count records 

How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.

- 85,567
- 105,567
- 125,567
- 145,567

```sh
  trip_count = spark.sql("""
    SELECT COUNT(*) AS trip_count
    FROM homework2025
    WHERE tpep_pickup_datetime BETWEEN '2024-10-15 00:00:00' AND '2024-10-15 23:59:59'
""")

trip_count.show()
+----------+
|trip_count|
+----------+
|    128893|
+----------+
```

## Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

- 122
- 142
- **162 <-**
- 182
  
```sh
spark.sql("""
SELECT tpep_pickup_datetime
,tpep_dropoff_datetime
,(unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) /3600 as hours
FROM homework2025
ORDER BY hours desc
""").show()
+--------------------+---------------------+------------------+
|tpep_pickup_datetime|tpep_dropoff_datetime|             hours|
+--------------------+---------------------+------------------+
| 2024-10-16 13:03:49|  2024-10-23 07:40:53|162.61777777777777|
```

## Question 5: User Interface

Spark’s User Interface which shows the application's dashboard runs on which local port?

- 80
- 443
- **4040<-**
- 8080



## Question 6: Least frequent pickup location zone

Load the zone lookup data into a temp view in Spark:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Using the zone lookup data and the Yellow October 2024 data, what is the name of the LEAST frequent pickup location Zone?

- **Governor's Island/Ellis Island/Liberty Island<-**
- Arden Heights
- Rikers Island
- Jamaica Bay

```sh
lookup_spark_df = spark.read.option("header", "true").csv("taxi_zone_lookup.csv")
lookup_spark_df.createOrReplaceTempView('lookup')

spark.sql("""
SELECT count(*) as cnt
,PUlocationID
,Borough
,Zone
FROM homework2025 inner join lookup
on PUlocationID = LocationID
GROUP BY PUlocationID, Borough, Zone
ORDER BY cnt asc
""").show()

+---+------------+-------------+--------------------+
|cnt|PUlocationID|      Borough|                Zone|
+---+------------+-------------+--------------------+
|  1|         105|    Manhattan|Governor's Island...|
|  2|           5|Staten Island|       Arden Heights|
```

## Submitting the solutions

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2025/homework/hw5
- Deadline: See the website
