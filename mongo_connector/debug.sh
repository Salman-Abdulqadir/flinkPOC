#!/bin/bash
echo "Cleaning the target folder and rebuilding"
mvn clean package shade:shade

# Flink Cluster Configuration
FLINK_CLUSTER_URL="http://flink-cluster-ip:rest-api-port"
JOB_JAR_PATH="/path/to/your/job.jar"
MAIN_CLASS="com.example.YourJobClass"
PROGRAM_ARGS=("arg1" "arg2")
PARALLELISM=4

# Step 1: Upload JAR
UPLOAD_RESPONSE=$(curl -X POST -H "Expect:" -F "jarfile=@${JOB_JAR_PATH}" "${FLINK_CLUSTER_URL}/jars/upload")
UPLOADED_JAR_ID=$(echo "${UPLOAD_RESPONSE}" | jq -r '.filename')

# Prompt the user
read -p "Do you want to submit the Flink job? (y/n): " choice
if [[ $choice != "y" ]]; then
  echo "Job submission cancelled."
  exit 0
fi

# Step 1: Upload JAR
UPLOAD_RESPONSE=$(curl -X POST -H "Expect:" -F "jarfile=@${JOB_JAR_PATH}" "${FLINK_CLUSTER_URL}/jars/upload")
UPLOADED_JAR_ID=$(echo "${UPLOAD_RESPONSE}" | jq -r '.filename')

# Step 2: Submit Job
JOB_SUBMISSION_PAYLOAD=$(jq -n --arg mainClass "$MAIN_CLASS" --argjson programArgs "${PROGRAM_ARGS[@]}" --argjson parallelism $PARALLELISM '{"entryClass":$mainClass, "programArgs":$programArgs, "parallelism":$parallelism}')
SUBMIT_RESPONSE=$(curl -X POST -H "Content-Type: application/json" -d "${JOB_SUBMISSION_PAYLOAD}" "${FLINK_CLUSTER_URL}/jars/${UPLOADED_JAR_ID}/run")
SUBMITTED_JOB_ID=$(echo "${SUBMIT_RESPONSE}" | jq -r '.jobid')

echo "Job submitted with ID: ${SUBMITTED_JOB_ID}"
