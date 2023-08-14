import pymongo
import time
import random

class Employee:
    def __init__(self, first_name, last_name, age, position):
        self.first_name = first_name
        self.last_name = last_name
        self.age = age
        self.position = position

class MongoDBEmployeeInserter:
    def __init__(self, mongodb_uri, database_name, collection_name):
        self.mongodb_uri = mongodb_uri
        self.database_name = database_name
        self.collection_name = collection_name
        self.client = pymongo.MongoClient(self.mongodb_uri)
        self.database = self.client[self.database_name]
        self.collection = self.database[self.collection_name]

    def insert_random_employee(self):
        first_names = ["Alice", "Bob", "Charlie", "David", "Emma", "Frank", "Grace"]
        last_names = ["Smith", "Johnson", "Williams", "Jones", "Brown", "Davis"]
        positions = ["Manager", "Developer", "Designer", "Accountant", "Salesperson"]
        
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        age = random.randint(22, 60)
        position = random.choice(positions)
        
        employee = Employee(first_name, last_name, age, position)
        self.collection.insert_one(employee.__dict__)
        print("Inserted employee:", employee.__dict__, "at:", time.strftime("%Y-%m-%d %H:%M:%S"))

    def run(self):
        try:
            i = 0
            while True:
                self.insert_random_employee()
                i+=1
                if i == 20:
                    i = 0
                    time.sleep(3)
        except KeyboardInterrupt:
            self.client.close()
            print("Program terminated.")

if __name__ == "__main__":
    mongodb_uri = "mongodb://localhost:27017/"
    database_name = "employeedb"
    collection_name = "employees"

    inserter = MongoDBEmployeeInserter(mongodb_uri, database_name, collection_name)
    inserter.run()
