#!/bin/bash

close(){
    echo "Stopping background programs and exiting"
    kill "$inserter_pid"
    exit 1
}

# signal interrupt handler
trap close SIGINT

echo "Inserting data to kafka: "
python3.9 data_inserters/main.py >> ./data/kafka_inserted_data.txt&
inserter_pid=$!

echo "Compiling flink job and submiting it to flink cluster on localhost:8081"
cd flink_jobs && sh submit_job.sh

echo "Data is being inserted to kafka. Logs -> (./data/kafka_inserted_data.txt).... "
# wait until all background program are finished before exiting
wait
echo "Program finished"
