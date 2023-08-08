from confluent_kafka import Producer
import json
import time
import random

class MovieRatingGenerator:
    def __init__(self, bootstrap_servers):
        self.bootstrap_servers = bootstrap_servers
        self.producer = Producer({"bootstrap.servers": self.bootstrap_servers})

    def generate_random_rating(self):
        movies = ["Movie A", "Movie B", "Movie C", "Movie D", "Movie E"]
        user_ids = [1, 2, 3, 4, 5]
        rating = round(random.uniform(1, 5), 1)
        movie = random.choice(movies)
        user_id = random.choice(user_ids)

        rating_data = {
            "movie": movie,
            "user_id": user_id,
            "rating": rating,
            "timestamp": int(time.time())
        }

        return rating_data

    def send_rating_to_kafka(self, rating_data):
        self.producer.produce("movie_ratings", json.dumps(rating_data).encode("utf-8"))
        self.producer.flush()

    def run(self):
        try:
            i = 0
            while True:
                rating_data = self.generate_random_rating()
                self.send_rating_to_kafka(rating_data)
                print("Sent rating:", rating_data, "to Kafka at:", time.strftime("%Y-%m-%d %H:%M:%S"))
                i+=1
                if (i == 10):
                    i = 0
                    time.sleep(10)
        except KeyboardInterrupt:
            print("Program terminated.")
            self.producer.flush()

if __name__ == "__main__":
    bootstrap_servers = "172.17.0.1:9094"
    
    rating_generator = MovieRatingGenerator(bootstrap_servers)
    rating_generator.run()
