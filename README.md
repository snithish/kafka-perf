# Kafka Checks

1. Clone the repository using 
```sh
git clone https://github.com/snithish/kafka-perf.git
cd kafka-perf
```

2. Create Python Virtualenv
```sh
python3 -m venv venv
```

3. Activate virtual env
```sh
source venv/bin/activate
```
4. Install dependencies
```sh
pip install -r requirements.txt
```

5. Download a large dataset we use the NYC Taxi Dataset (1GB)
```sh
 wget https://nyc-tlc.s3.amazonaws.com/trip+data/fhvhv_tripdata_2022-02.csv
```

6. Use docker compose to bring up kafka
```sh
docker-compose up -d --force-recreate
```

7. Use the below command to create a topic
```sh
docker exec --interactive --tty broker \
kafka-console-producer --bootstrap-server broker:9092 \
                       --topic quickstart
```

7. Run the producer script, you can edit script to increase rows sent to kafka
```sh
python3 producer.py
```