from pyspark.sql import SparkSession
import json
import sys
import time

def main(review_path, business_path, output_a, output_b):

    spark = SparkSession.builder.appName('task3').getOrCreate()

    sc = spark.sparkContext


    # M2 time
    M2_start = time.time()    
    raw_review = sc.textFile(review_path)
    raw_business = sc.textFile(business_path)

    json_review = raw_review.map(lambda line: json.loads(line))
    json_business = raw_business.map(lambda line: json.loads(line))

    # Question A, business_id is Primary Key in business

    review_KV = json_review.map(lambda x: (x["business_id"], (x["stars"], 1)))
    business_KV = json_business.map(lambda x: (x["business_id"], x["city"]))

    join_rdd = review_KV.leftOuterJoin(business_KV)

    # input (bus_id, (city, (stars, 1)))
    city_star_sum_rdd = join_rdd.map(lambda x: (x[1][1], x[1][0])).reduceByKey(lambda a, b: (a[0]+b[0], a[1] + b[1]))

    #average

    avg_star_city = city_star_sum_rdd.mapValues(lambda x: x[0] / x[1]).sortBy(lambda x: (-x[1], x[0])).collect() 

    top_10_city = avg_star_city[:10]

    print(top_10_city)

    M2_time = time.time() - M2_start


    

    # Write output to file
    with open(output_a, 'w') as file:
        file.write("city,stars\n")  # Header
        for city, stars in avg_star_city:
            file.write(f"{city},{stars}\n") 


   # Question B

    # M1 time
    M1_start = time.time()    
    raw_review = sc.textFile(review_path)
    raw_business = sc.textFile(business_path)

    json_review = raw_review.map(lambda line: json.loads(line))
    json_business = raw_business.map(lambda line: json.loads(line))

    # Question A, business_id is Primary Key in business

    review_KV = json_review.map(lambda x: (x["business_id"], (x["stars"], 1)))
    business_KV = json_business.map(lambda x: (x["business_id"], x["city"]))

    join_rdd = review_KV.leftOuterJoin(business_KV)

    # input (city, (stars, 1))
    city_star_sum_rdd = join_rdd.map(lambda x: (x[1][1], x[1][0])).reduceByKey(lambda a, b: (a[0]+b[0], a[1] + b[1]))

    #average
    avg_star_city = city_star_sum_rdd.mapValues(lambda x: x[0] / x[1]).collect()

    #use build-in sorted

    sorted_data = sorted(avg_star_city, key=lambda x: (-x[1], x[0]))

    print(sorted_data[:10])

    M1_time = time.time() - M1_start



    result = {
        "m1": M1_time,
        "m2": M2_time,
        "reason": "After running on Local, I found using Python sorted function is faster than Spark due to the partition of Spark. When sort the whole data it needs to copy through different partition, this kind of shuffling slows the runtime. "

    }



    with open(output_b, 'w') as output_file:
        json.dump(result, output_file)





    spark.stop()

if __name__ == "__main__":
    
    review_path = sys.argv[1]
    business_path = sys.argv[2]
    output_a = sys.argv[3]
    output_b = sys.argv[4]
    main(review_path, business_path, output_a, output_b)