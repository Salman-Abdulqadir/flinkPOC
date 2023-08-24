#!/bin/bash

echo "Cleaning the target folder and rebuilding a FAT jar file"
mvn clean package shade:shade

echo "Uploading flink job and submit a flink job"
FLINK_CLUSTER_URL="http://localhost:8081"
JOB_JAR_PATH="/Users/s.abdulqadir/Documents/flinkPOC/flink_jobs/target/flink-jobs-1.0.0.jar"
ENTRY_CLASS="ai.saal.Main"

# uploading the jar file to flink cluster
UPLOAD_RESPONSE=$(curl -s -X POST -H "Expect:" -F "jarfile=@${JOB_JAR_PATH}" "${FLINK_CLUSTER_URL}/jars/upload")
UPLOADED_JAR_URL=$(echo "${UPLOAD_RESPONSE}" | json filename)
UPLOADED_JAR_ID=$(basename "$UPLOADED_JAR_URL")

# Submit the Flink job using the uploaded JAR
JOB_SUBMIT_RESPONSE=$(curl -s -X POST "${FLINK_CLUSTER_URL}/jars/${UPLOADED_JAR_ID}/run?entry-class=${ENTRY_CLASS}")
JOB_ID=$(echo "${JOB_SUBMIT_RESPONSE}" | json jobid)

echo "Job submitted successfully. Job_ID = ${JOB_ID}"
