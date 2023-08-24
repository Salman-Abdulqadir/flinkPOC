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

echo "Compiling flink job"
sh ./mongo_connector/debug.sh


# wait until all background program are finished before exiting
wait
echo "Program finished"