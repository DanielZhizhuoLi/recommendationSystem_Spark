from pyspark.sql import SparkSession
import json
import sys
import math


def compute_pearson(ratings_b, ratings_i, min_common=1):
    common_users = set(ratings_b.keys()) & set(ratings_i.keys())

    # not consider when there is only on customer
    if len(common_users) < min_common:
        return 0.0

    avg_b = sum(ratings_b[u] for u in common_users) / len(common_users)
    avg_i = sum(ratings_i[u] for u in common_users) / len(common_users)

    numerator = sum((ratings_b[u] - avg_b) * (ratings_i[u] - avg_i) for u in common_users)
    denominator_b = math.sqrt(sum((ratings_b[u] - avg_b) ** 2 for u in common_users))
    denominator_i = math.sqrt(sum((ratings_i[u] - avg_i) ** 2 for u in common_users))
    denominator = denominator_b * denominator_i

    if denominator == 0:
        if numerator == 0:
            return 1.0
        else:
            return -1.0
    return min(1.0, numerator / denominator)


def predict_rating_for_pair(user, business, business_sets, user_sets, business_means, user_means, overall_avg, neighbor_limit=15):

    # if business_id not in the test set

    if business not in business_sets:
        if user in user_sets:
            return user_means.get(user, overall_avg)
        else:
            return overall_avg
    # if user_id not in the test set
    if user not in user_sets:
        return business_means.get(business, overall_avg)

    neighbor_list = []
    user_ratings = user_sets[user]
    for b_i, rating in user_ratings.items():
        if b_i == business:
            continue
        if b_i in business_sets:
            sim = compute_pearson(business_sets[business], business_sets[b_i])
            if sim < 0:
                sim = sim * 0.8
            else:
                sim = sim * 1.2

            normalized_rating = rating - (business_means.get(b_i, overall_avg) + user_means.get(user, overall_avg) - overall_avg)
            neighbor_list.append((sim, normalized_rating))

    neighbor_list = sorted(neighbor_list, key=lambda x: x[0], reverse=True)[:neighbor_limit]

    numerator = sum(sim * rating for sim, rating in neighbor_list)
    denominator = sum(abs(sim) for sim, rating in neighbor_list)

    baseline = overall_avg + (business_means.get(business, overall_avg) - overall_avg) + (user_means.get(user, overall_avg) - overall_avg)

    if denominator != 0:
        prediction = baseline + numerator / denominator
    else:
        prediction = baseline

    return min(max(prediction, 1.0), 5.0)



def main(train_file, test_file, output_file):
    spark = SparkSession.builder.appName("task2_1").getOrCreate()
    sc = spark.sparkContext

    rdd = sc.textFile(train_file)
    header = rdd.first()

    business_rdd = rdd.filter(lambda x: x != header).map(lambda x: x.split(',')).map(lambda x: (x[1], (x[0], float(x[2]))))
    business_sets = business_rdd.groupByKey().mapValues(lambda vals: {u: r for u, r in vals}).collectAsMap()

    business_means = {b: sum(ratings.values()) / len(ratings) for b, ratings in business_sets.items()}

    user_rdd = rdd.filter(lambda x: x != header).map(lambda x: x.split(',')).map(lambda x: (x[0], (x[1], float(x[2]))))
    user_sets = user_rdd.groupByKey().mapValues(lambda vals: {b: r for b, r in vals}).collectAsMap()

    overall_avg = rdd.filter(lambda x: x != header).map(lambda x: float(x.split(',')[2])).mean()

    user_means = {u: sum(ratings.values()) / len(ratings) for u, ratings in user_sets.items()}
    
    
    broadcast_user_means = sc.broadcast(user_means)
    broadcast_business = sc.broadcast(business_sets)
    broadcast_user = sc.broadcast(user_sets)
    broadcast_business_means = sc.broadcast(business_means)
    broadcast_overall = sc.broadcast(overall_avg)

    test_rdd = sc.textFile(test_file)
    test_header = test_rdd.first()

    test_data = test_rdd.filter(lambda x: x != test_header).map(lambda x: x.split(',')).map(lambda x: (x[0], x[1]))

    predictions = test_data.map(lambda pair: (
        pair[0],
        pair[1],
        predict_rating_for_pair(
            pair[0],
            pair[1],
            broadcast_business.value,
            broadcast_user.value,
            broadcast_business_means.value,
            broadcast_user_means.value,  
            broadcast_overall.value,
            neighbor_limit=50
        )
    ))


    output = predictions.collect()
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("user_id,business_id,prediction\n")
        for (b1, b2, sim) in output:
            f.write(f"{b1},{b2},{sim}\n")

    spark.stop()


if __name__ == "__main__":
    train_file = sys.argv[1]
    test_file = sys.argv[2]
    output_file = sys.argv[3]
    main(train_file, test_file, output_file)
