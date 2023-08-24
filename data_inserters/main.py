import sys
import os

curr_dir = os.path.dirname(os.path.abspath(__file__))
modules_path = os.path.join(curr_dir, "src")

sys.path.append(modules_path)

from KafkaInserter import MovieRatingGenerator
import threading

def main():

    bootstrap_server = "localhost:29092"

    # rating generator 1 has 5 attributes, and rating generator 2 has 6 attributes
    rating_generator1 = MovieRatingGenerator(bootstrap_server, "v1", 10)
    rating_generator2 = MovieRatingGenerator(bootstrap_server, "v2", 3)

    rating_thread1 = threading.Thread(target=rating_generator1.run)
    rating_thread2 = threading.Thread(target=rating_generator2.run)

    rating_thread1.start()
    rating_thread2.start()

    rating_thread1.join()
    rating_thread2.join()

    
    # todos
    # K2K (call suhas after finishing)
    # k2C as two separate connectors
    # run two threads for the same entity that inserts different number of attributes, and the flink job should accept it
    # document how the data is saved in cassandra sink
  
if __name__ == "__main__":
    main()





