from pyspark.sql import SparkSession
import json
import sys




def main(input_filepath, output_filepath):
    # get session
    spark = SparkSession.builder.appName('task1').getOrCreate()
    sc = spark.sparkContext


    # get json
    rdd = sc.textFile(input_filepath)

    json_rdd = rdd.map(lambda x: json.loads(x))

    #a
    n_review = json_rdd.map(lambda x : 1).reduce(lambda a, b: a+b)

    #b
    n_review_2018 = json_rdd.filter(lambda x: x['date'].startswith('2018') ).map(lambda x : 1).reduce(lambda a, b: a+b)

    #c
    n_user = json_rdd.map(lambda x: x['user_id']).distinct().count()

    #d
    top10_user = json_rdd.map(lambda x: (x['user_id'], 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: (-x[1], x[0])).take(10)
    top10_user_list = [[user, count] for user, count in top10_user]

    #e
    n_business = json_rdd.map(lambda x: x['business_id']).distinct().count()

    #f
    top10_business = json_rdd.map(lambda x: (x['business_id'], 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: (-x[1], x[0])).take(10)
    top10_business_list = [[user, count] for user, count in top10_business]

    results = {
        "n_review": n_review,
        "n_review_2018": n_review_2018,
        "n_user": n_user,
        "top10_user": top10_user_list,
        "n_business": n_business,
        "top10_business": top10_business_list
    }

    print(results)

    with open(output_filepath, 'w') as output_file:
        json.dump(results, output_file, indent=4)

    spark.stop()

if __name__ == "__main__":
    input_filepath = sys.argv[1]
    output_filepath = sys.argv[2]
    main(input_filepath, output_filepath)


