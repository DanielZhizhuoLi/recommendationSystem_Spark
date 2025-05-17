import json
import sys
from itertools import combinations
import math
import random
import binascii
from blackbox import BlackBox


BIT_ARRAY_SIZE = 69997
NUM_HASH_FUNCTIONS = 10
bit_array = [0] * BIT_ARRAY_SIZE
true_users = set()


# ((ax + b) % p) % m
def generate_hash(m = BIT_ARRAY_SIZE, num_hush = NUM_HASH_FUNCTIONS):

    prime_list = [50021, 50023, 50033, 50047, 50051, 50069, 50077, 50087, 50093, 50101]
    # prime_list = [5003, 5009, 5011, 5021, 5023, 5039, 5051, 5059, 5077, 5081]
    hash_functions=[]

    for _ in range(num_hush):
        p = random.choice(prime_list)
        a = random.randint(1, p-1)
        b = random.randint(1, p-1)

        def hash_func(x, a=a, b=b, p=p):
            return ((a * x + b) % p) % m
        
        hash_functions.append(hash_func)
    
    return hash_functions

global_hash_functions = generate_hash()


# get the result of hash 
def myhashs(s):
    hash_results = []
    input_number = int(binascii.hexlify(s.encode('utf8')), 16)

    for hash_func in global_hash_functions:
        hash_results.append(hash_func(input_number))

    return hash_results
    


def main(input_file, stream_size, num_of_asks, output_file):

    bx = BlackBox()
    hash_functions = generate_hash()

    with open(output_file, 'w') as f_out:
        f_out.write("Time,FPR\n")

        for i in range(num_of_asks):
            user_steam = bx.ask(input_file, stream_size)

            false_positive = 0
            total_negative = 0

            for user_id in user_steam:
                hashes = myhashs(user_id)
                seen = all(bit_array[h] == 1 for h in hashes)

                if seen and user_id not in true_users:
                    false_positive += 1

                if user_id not in true_users:
                    total_negative += 1
                    true_users.add(user_id)
                    for h in hashes:
                        bit_array[h] = 1

            fpr = false_positive / total_negative if total_negative > 0 else 0

            f_out.write(f"{i},{fpr}\n")



if __name__ == "__main__":
    input_file = sys.argv[1]
    stream_size = int(sys.argv[2])
    num_of_asks = int(sys.argv[3])
    output_file = sys.argv[4]
    main(input_file, stream_size, num_of_asks, output_file)