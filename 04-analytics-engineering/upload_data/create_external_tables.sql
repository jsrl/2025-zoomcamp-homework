CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.yellow_tripdata`
OPTIONS (
  format = 'CSV',
  uris = [
    'gs://taxis-bucket-448121-i4/new_taxis/yellow_tripdata_*.csv.gz'
  ],
  compression = 'GZIP'
);

CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.green_tripdata`
OPTIONS (
  format = 'CSV',
  uris = [
    'gs://taxis-bucket-448121-i4/new_taxis/green_tripdata_*.csv.gz'
  ],
  compression = 'GZIP'
);

CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = [
    'gs://taxis-bucket-448121-i4/new_taxis/fhv_tripdata_*.csv.gz'
  ],
  compression = 'GZIP'
);


select count(*) from `trips_data_all.yellow_tripdata`;
select count(*) from `trips_data_all.green_tripdata`;
select count(*) from `trips_data_all.fhv_tripdata`;