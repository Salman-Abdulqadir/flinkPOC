#!/bin/bash
echo "Cleaning the target folder and rebuilding"
mvn clean package shade:shade

echo "changing java version to JDK 8"
export JAVA_HOME=`/usr/libexec/java_home -v 1.8`

echo "debugging jar file using JDK 8"
java -jar target/flink-mongodb-connector-1.0.0.jar

echo "Switching java version to JDK 20"
export JAVA_HOME=`/usr/libexec/java_home -v 20`