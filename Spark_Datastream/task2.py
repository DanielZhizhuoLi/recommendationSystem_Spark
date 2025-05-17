from blackbox import BlackBox
import sys
import random
import binascii

BIT_ARRAY_SIZE = 69997
NUM_HASH_FUNCTIONS = 10  # Using 10 hash functions

# ((ax + b) % p) % m
def generate_hash(m = BIT_ARRAY_SIZE, num_hush = NUM_HASH_FUNCTIONS):

    prime_list = [997, 991, 983, 977, 971, 967, 953, 947, 941, 937]

    hash_functions = []
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

    def count_trailing_zeros(x):
        if x == 0:
            return 0  
        count = 0
        while x % 2 == 0:
            count += 1
            x //= 2
        return count

    total_estimation = 0.0
    total_ground_truth = 0.0

    with open(output_file, 'w') as f_out:

        f_out.write("Time,Ground Truth,Estimation\n")
        
        for i in range(num_of_asks):
            stream = bx.ask(input_file, stream_size)
            ground_truth = len(set(stream))
            total_ground_truth += ground_truth
            
            R = [0] * NUM_HASH_FUNCTIONS
            

            for user in stream:
                hvals = myhashs(user)
                for j, h in enumerate(hvals):
                    tz = count_trailing_zeros(h)
                    if tz > R[j]:
                        R[j] = tz
            

            estimates = [2 ** r for r in R]

            group_size = 10
            group_avgs = []
            for k in range(0, len(estimates), group_size):
                group = estimates[k:k+group_size]
                group_avg = sum(group) / float(len(group))
                group_avgs.append(group_avg)
            group_avgs.sort()
            
            # take the median of the group averages:
            n = len(group_avgs)
            if n % 2 == 1:
                fm_estimate = group_avgs[n // 2]
            else:
                fm_estimate = (group_avgs[n // 2 - 1] + group_avgs[n // 2]) / 2.0
            
            fm_estimate = int(round(fm_estimate))

            total_estimation += fm_estimate
            
            f_out.write(f"{i},{ground_truth},{fm_estimate}\n")

        
        # Calculate and print the overall ratio.
        if total_ground_truth > 0:
            overall_ratio = total_estimation / total_ground_truth
            print(f"\n(sum of estimations / sum of ground truths): {overall_ratio}")
        else:
            print("\nNo ground truth values were accumulated.")

if __name__ == "__main__":
    input_file = sys.argv[1]
    stream_size = int(sys.argv[2])
    num_of_asks = int(sys.argv[3])
    output_file = sys.argv[4]   
    main(input_file, stream_size, num_of_asks, output_file)
