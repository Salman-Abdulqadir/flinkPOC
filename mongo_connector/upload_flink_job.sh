# Flink Cluster Configuration
FLINK_CLUSTER_URL="http://localhost:8081"
JOB_JAR_PATH="target/flink-mongodb-connector-1.0.0.jar"

# Step 1: Upload JAR
UPLOAD_RESPONSE=$(curl -X POST -H "Expect:" -F "jarfile=@${JOB_JAR_PATH}" "${FLINK_CLUSTER_URL}/jars/upload")
UPLOADED_JAR_ID=$(echo "${UPLOAD_RESPONSE}" | json filename)
echo "Job uploaded with ID: ${UPLOADED_JAR_ID}"
