import csv
import json
from kafka import KafkaProducer
from time import time

def main():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    fields = [
        'lpep_pickup_datetime',
        'lpep_dropoff_datetime',
        'PULocationID',
        'DOLocationID',
        'passenger_count',
        'trip_distance',
        'tip_amount'
    ]

    csv_file = 'data/green_tripdata_2019-10.csv'
    with open(csv_file, 'r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)

        for row in reader:
            filtered_row = {key: row[key] for key in fields if key in row}
            producer.send('green-trips', value=filtered_row)
    producer.flush()
    producer.close()


if __name__ == "__main__":
    t0 = time()
    main()
    t1 = time()
    duration = t1 - t0
    print(f"Duration: {duration}")