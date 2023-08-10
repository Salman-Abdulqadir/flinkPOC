echo "Cleaning the target folder and rebuilding"
mvn clean package shade:shade \ &&

echo "\n Running jar"
java -jar target/flink-mongodb-connector-1.0.0.jar