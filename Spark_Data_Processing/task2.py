from pyspark.sql import SparkSession
import json
import sys
import time


def main(input_filepath, output_filepath, partition):

    spark = SparkSession.builder.appName("task2").getOrCreate()

    sc = spark.sparkContext


    # get the file
    rdd = sc.textFile(input_filepath)

    # convert it to json_rdd
    json_rdd = rdd.map(lambda x: json.loads(x))


    
    start_time_d = time.time()
    #Question f
    top10_business = json_rdd.map(lambda x: (x['business_id'], 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: (-x[1], x[0])).take(10)
    exe_time_d = time.time() - start_time_d

    #default
    n_partition = json_rdd.getNumPartitions()
    default_n_items = json_rdd.mapPartitions(lambda partition: [sum(1 for _ in partition)]).collect()

    
    # customized 
    #partitionBy default portable_hash 
    customized_rdd = json_rdd.map(lambda x: (x['business_id'], 1)).partitionBy(partition)
    



    customized_n_partition = customized_rdd.getNumPartitions()
    customized_n_items = customized_rdd.mapPartitions(lambda partition: [sum(1 for _ in partition)]).collect()


    start_time_c = time.time()
    customized_top10 = customized_rdd.reduceByKey(lambda a, b: a + b).sortBy(lambda x: (-x[1], x[0])).take(10)
    exe_time_c = time.time() - start_time_c
    

    result = {
    "default": {
        "n_partition": n_partition,
        "n_items": default_n_items,
        "exe_time": exe_time_d
    },
    "customized": {
        "n_partition": customized_n_partition,
        "n_items": customized_n_items,
        "exe_time": exe_time_c
    }
}

    print(result)

    with open(output_filepath, 'w') as output_file:
        json.dump(result, output_file)

    spark.stop()

if __name__ == "__main__":
    input_filepath = sys.argv[1]
    output_filepath = sys.argv[2]
    partition = int(sys.argv[3])
    main(input_filepath, output_filepath, partition)
