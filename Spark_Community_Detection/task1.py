from pyspark.sql import SparkSession
import json
import sys
from itertools import combinations
from graphframes import GraphFrame
import math



def main(threshold, input_file, output_file):

    spark = SparkSession.builder.appName("task1").getOrCreate()
    sc = spark.sparkContext

    data = sc.textFile(input_file)
    header = data.first()
    rdd = data.filter(lambda line: line != header).map(lambda line: line.split(',')).map(lambda row: (row[0], row[1]))


    #get user:{business1, buiness2}
    user_set = rdd.groupByKey().mapValues(lambda x: set(x))

    # get business1:user1
    business_user_rdd = user_set.flatMap(lambda x: [(b, x[0]) for b in x[1]])
    
    # get business1:[user1, user2, user3]
    business_grouped = business_user_rdd.groupByKey().mapValues(lambda users: list(users))
    # get (user1, user2) pairs
    user_pairs_rdd = business_grouped.flatMap(lambda x: [tuple(sorted(pair)) for pair in combinations(x[1], 2)])

    pairs_rdd = user_pairs_rdd.map(lambda pair: (pair, 1)).reduceByKey(lambda a, b: a + b).filter(lambda x: x[1] >= threshold).keys()

    edges_rdd = pairs_rdd.flatMap(lambda pair: [(pair[0], pair[1]), (pair[1], pair[0])]).distinct()

    edges = spark.createDataFrame(edges_rdd, schema=["src", "dst"])
    vertices_rdd = edges_rdd.flatMap(lambda x: x).distinct().map(lambda u: (u,))
    vertices = spark.createDataFrame(vertices_rdd, schema=["id"])



    graphframe = GraphFrame(vertices, edges)
    communities = graphframe.labelPropagation(maxIter=5)

    communities_list = communities.rdd.map(lambda row: (row['label'], [row['id']])) .reduceByKey(lambda x, y: x + y).map(lambda x: sorted(x[1])).sortBy(lambda members: (len(members), members[0])).collect()


    with open(output_file, "w", encoding="utf-8") as f:
        lines = []
        for community in communities_list:
            line = ",".join([f"'{user}'" for user in community])
            lines.append(line)
        f.write("\n".join(lines))


    spark.stop()



if __name__ == "__main__":

    threshold = int(sys.argv[1])
    input_file = sys.argv[2]
    output_file = sys.argv[3]
    main(threshold, input_file, output_file)

