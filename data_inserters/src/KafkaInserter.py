from confluent_kafka import Producer
import json
import time
import random

class MovieRatingGenerator:
    def __init__(self, bootstrap_servers, rating_format =  "v1", insertion_interval = 10):
        self.bootstrap_servers = bootstrap_servers
        self.rating_format = rating_format
        self.insertion_interval = insertion_interval
        self.producer = Producer({"bootstrap.servers": self.bootstrap_servers})


    def generate_rating1(self):
        movies = ["Movie A", "Movie B", "Movie C", "Movie D", "Movie E"]
        rating = round(random.uniform(1, 5), 1)
        movie = random.choice(movies)
        user_id = random.randrange(1, 1999)

        if (self.rating_format== "v1"):
            rating_data = {
                "movie": movie,
                "user_id": user_id,
                "rating": rating,
                "timestamp": int(time.time())
            }
        else:
             movie_format = random.choice(["MP4","AVI","MKV","MOV","WMV","FLV"])
             lead_actor = random.choice(["John", "Doe", "Mike", "Jennifer", "Julia", "Gasper"])
             rating_data = {
                "movie": movie,
                "user_id": user_id,
                "rating": rating,
                "timestamp": int(time.time()),
                "format": movie_format,
                "actor": lead_actor
            }

        return rating_data

    def send_rating_to_kafka(self, rating_data):
        self.producer.produce("movie_ratings", json.dumps(rating_data).encode("utf-8"))
        self.producer.flush()

    def run(self):
        try:
            i = 0
            while True:
                rating_data = self.generate_rating1()
                self.send_rating_to_kafka(rating_data)
                print(f"Sent {self.rating_format} formated rating:", rating_data, "to Kafka at:", time.strftime("%Y-%m-%d %H:%M:%S"))
                i+=1
                if (i == 10):
                    i = 0
                    time.sleep(self.insertion_interval)
        except KeyboardInterrupt:
            print("Program terminated.")
            self.producer.flush()

if __name__ == "__main__":
    bootstrap_servers = "localhost:29092"
    rating_generator = MovieRatingGenerator(bootstrap_servers, "v2")
    rating_generator.run()
