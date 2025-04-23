from pyspark.sql import SparkSession
import json
import sys
import numpy as np
from xgboost import XGBRegressor
import os


def parse_user_json(line):
    user = json.loads(line)
    
    user_id = user["user_id"]
    review_count = user.get("review_count", 0)
    avg_stars = user.get("average_stars", 0.0)
    useful = user.get("useful", 0)

    # (user_id, feature_vector)
    return (user_id, [
        review_count,
        avg_stars,
        useful
    ])

def parse_business_json(line):
    business = json.loads(line)

    business_id = business["business_id"]
    stars = business.get("stars", 0.0)
    review_count = business.get("review_count", 0)

    attributes = business.get("attributes", {})
    price_range_str = attributes.get("RestaurantsPriceRange2", "0") if attributes else "0"
    try:
        price_range = int(price_range_str)
    except:
        price_range = 0

    return (business_id, [stars, review_count, price_range])


def build_feature_vector(user_id, business_id, user_dict, business_dict):
    user_features = user_dict.get(user_id, [0, 0.0, 0])  # review_count, avg_stars, useful
    business_features = business_dict.get(business_id, [0.0, 0, 0])  # stars, review_count, price_range
    return user_features + business_features


def main(folder_path, test_file, output_file):
    spark = SparkSession.builder.appName("task2_2").getOrCreate()
    sc = spark.sparkContext

    user_rdd = sc.textFile(os.path.join(folder_path, 'user.json'))
    business_rdd = sc.textFile(os.path.join(folder_path, 'business.json'))


    train_rdd = sc.textFile(os.path.join(folder_path,'yelp_train.csv'))
    train_header = train_rdd.first()
    train_rdd = train_rdd.filter(lambda x: x != train_header)
    
    user_to_train = train_rdd.map(lambda x: x.split(',')[0]).distinct().collect()
    business_to_train = train_rdd.map(lambda x: x.split(',')[1]).distinct().collect()

    user_to_train_b = sc.broadcast(set(user_to_train))
    business_to_train_b = sc.broadcast(set(business_to_train))

    user_features_rdd = user_rdd.filter(lambda line: json.loads(line)["user_id"] in user_to_train_b.value)\
        .map(parse_user_json)


    business_features_rdd = business_rdd.filter(lambda line: json.loads(line)["business_id"] in business_to_train_b.value)\
        .map(parse_business_json)

    business_features_dict = dict(business_features_rdd.collect())
    user_features_dict = dict(user_features_rdd.collect())


    training_data = train_rdd.map(lambda line: line.split(','))\
        .map(lambda x: (x[0], x[1], float(x[2])))\
        .map(lambda x: (build_feature_vector(x[0], x[1], user_features_dict, business_features_dict), x[2]))\
        .collect()


    
    X_train = np.array([item[0] for item in training_data])
    y_train = np.array([item[1] for item in training_data])


    model = XGBRegressor(max_depth=6, learning_rate=0.1, n_estimators=100, objective='reg:linear')
    model.fit(X_train, y_train)

    # test data
    test_rdd = sc.textFile(test_file)
    test_header = test_rdd.first()
    test_rdd = test_rdd.filter(lambda x: x != test_header)

    test_data = test_rdd.map(lambda line: line.split(',')).map(lambda x: (x[0], x[1], build_feature_vector(x[0], x[1], user_features_dict, business_features_dict))).collect()


    X_test = np.array([item[2] for item in test_data])
    y_pred = model.predict(X_test)


    with open(output_file, "w", encoding="utf-8") as f:
        f.write("user_id,business_id,prediction\n")
        for i in range(len(test_data)):
            f.write(f"{test_data[i][0]},{test_data[i][1]},{y_pred[i]}\n")

    spark.stop()

if __name__ == "__main__":
    folder_path = sys.argv[1]
    test_file = sys.argv[2]
    output_file = sys.argv[3]
    main(folder_path, test_file, output_file)