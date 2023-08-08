from MongoInserter import MongoDBEmployeeInserter
from KafkaInserter import MovieRatingGenerator
import threading

def main():
    mongodb_uri = "mongodb://localhost:27017/"
    database_name = "employeedb"
    collection_name = "employees"
    inserter = MongoDBEmployeeInserter(mongodb_uri, database_name, collection_name)

    bootstrap_servers = "localhost:9092"
    rating_generator = MovieRatingGenerator(bootstrap_servers)

    employee_thread = threading.Thread(target=inserter.run)
    rating_thread = threading.Thread(target=rating_generator.run)

    employee_thread.start()
    rating_thread.start()

    employee_thread.join()
    rating_thread.join()

if __name__ == "__main__":
    main()





