from pyspark.sql import SparkSession
import json
import sys
import random



def is_prime(n):
    if n <= 1:
        return False
    for i in range(2, int(n**0.5) + 1):
        if n % i == 0:
            return False
    return True

def next_prime(n):
    while True:
        if is_prime(n):
            return n
        n += 1



def compute_signature(user_set, user_index_dict, hash_funcs):
    
    signature = [float('inf')] * len(hash_funcs)
    for user in user_set:
        if user in user_index_dict:
            user_index = user_index_dict[user]
            for i, f in enumerate(hash_funcs):
                h_val = f(user_index)
                if h_val < signature[i]:
                    signature[i] = h_val
    return signature

def split_into_bands(business_id, signature, b, r):
    bands = []
    for i in range(b):
        band = tuple(signature[i * r : (i + 1) * r])
        band_key = (i, hash(band))
        bands.append((band_key, business_id))
    return bands

def generate_candidate_pairs(business_list):
    business_list = sorted(business_list) 
    pairs = []
    n = len(business_list)
    for i in range(n):
        for j in range(i+1, n):
            pairs.append((business_list[i], business_list[j]))
    return pairs


def main(input_file, output_filepath):
    spark = SparkSession.builder.appName("task1").getOrCreate()
    sc = spark.sparkContext

    rdd  = sc.textFile(input_file)

    header = rdd.first()

    kv_rdd = rdd.filter(lambda x: x != header).map(lambda x: x.split(',')).map(lambda x: (x[1], x[0]))


    # convert to set in order to speed up the i/o 
    business_sets = kv_rdd.mapValues(lambda x: {x}).reduceByKey(lambda set1, set2: set1 | set2)

    # give a fix permutation
    all_users = kv_rdd.map(lambda x: x[1]).distinct()

    user_permutation = all_users.zipWithIndex().collectAsMap()

    # setup hash function
    num_users = len(user_permutation)
    m = next_prime(num_users)
    num_hash_funcs = 100 

    hash_funcs = []
    for _ in range(num_hash_funcs):
        a = random.randint(1, m - 1)
        b = random.randint(0, m - 1)

        hash_funcs.append(lambda x, a=a, b=b: (a * x + b) % m)

    business_signatures = business_sets.mapValues(
        lambda users: compute_signature(users, user_permutation, hash_funcs)
    )

    b = 20
    r = 100 // b

    bands_rdd = business_signatures.flatMap(lambda x: split_into_bands(x[0], x[1], b, r))

    grouped_bands = bands_rdd.groupByKey().mapValues(list)


    candidate_pairs = grouped_bands.flatMap(lambda x: generate_candidate_pairs(x[1])).distinct()


    business_users_dict = sc.broadcast(business_sets.collectAsMap())

    def jaccard_similarity(pair):
        b1, b2 = pair
        users1 = business_users_dict.value[b1]
        users2 = business_users_dict.value[b2]
        intersection = len(users1.intersection(users2))
        union = len(users1.union(users2))
        return float(intersection) / union if union != 0 else 0.0


    candidate_similarities = candidate_pairs.map(lambda pair: (pair[0], pair[1], jaccard_similarity(pair)))


    similar_pairs_sorted = candidate_similarities.filter(lambda x: x[2] >= 0.5).sortBy(lambda x: (x[0], x[1], x[2]))


    sorted_list = similar_pairs_sorted.collect()


    with open(output_filepath, "w", encoding="utf-8") as f:
        f.write("business_id_1,business_id_2,similarity\n")
        for (b1, b2, sim) in sorted_list:
            f.write(f"{b1},{b2},{sim}\n")

    spark.stop()



if __name__ == "__main__":

    input_file = sys.argv[1]
    out_file = sys.argv[2]
    main(input_file, out_file)


