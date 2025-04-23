from pyspark.sql import SparkSession
import json
import sys
import math
import numpy as np
from xgboost import XGBRegressor
import os

# ----------------- 协同过滤部分 -----------------
def compute_pearson(ratings_b, ratings_i, min_common=1):
    common_users = set(ratings_b.keys()) & set(ratings_i.keys())
    if len(common_users) < min_common:
        return 0.0
    avg_b = sum(ratings_b[u] for u in common_users) / len(common_users)
    avg_i = sum(ratings_i[u] for u in common_users) / len(common_users)
    numerator = sum((ratings_b[u] - avg_b) * (ratings_i[u] - avg_i) for u in common_users)
    denominator_b = math.sqrt(sum((ratings_b[u] - avg_b) ** 2 for u in common_users))
    denominator_i = math.sqrt(sum((ratings_i[u] - avg_i) ** 2 for u in common_users))
    denominator = denominator_b * denominator_i
    if denominator == 0:
        return 1.0 if numerator == 0 else -1.0
    return min(1.0, numerator / denominator)

def predict_rating_for_pair_cf(user, business, business_sets, user_sets, business_means, user_means, overall_avg, neighbor_limit=50):
    # 如果业务或用户不存在，则返回对应均值
    if business not in business_sets:
        if user in user_sets:
            return user_means.get(user, overall_avg), 0
        else:
            return overall_avg, 0
    if user not in user_sets:
        return business_means.get(business, overall_avg), 0

    neighbor_list = []
    user_ratings = user_sets[user]
    for b_i, rating in user_ratings.items():
        if b_i == business:
            continue
        if b_i in business_sets:
            sim = compute_pearson(business_sets[business], business_sets[b_i])
            # 对相似度进行放大或缩小的权重调整
            sim = sim * 0.8 if sim < 0 else sim * 1.2
            # 归一化评分
            normalized_rating = rating - (business_means.get(b_i, overall_avg) + user_means.get(user, overall_avg) - overall_avg)
            neighbor_list.append((sim, normalized_rating))
    # 按相似度降序排序，并截取最多 neighbor_limit 个邻居
    neighbor_list = sorted(neighbor_list, key=lambda x: x[0], reverse=True)
    neighbors_used = neighbor_list[:neighbor_limit]
    neighbor_count = len(neighbors_used)
    numerator = sum(sim * rating for sim, rating in neighbors_used)
    denominator = sum(abs(sim) for sim, rating in neighbors_used)
    baseline = overall_avg + (business_means.get(business, overall_avg) - overall_avg) + (user_means.get(user, overall_avg) - overall_avg)
    if denominator != 0:
        prediction = baseline + numerator / denominator
    else:
        prediction = baseline
    prediction = min(max(prediction, 1.0), 5.0)
    return prediction, neighbor_count

# ----------------- 模型方法部分 -----------------
def parse_user_json(line):
    user = json.loads(line)
    user_id = user["user_id"]
    review_count = user.get("review_count", 0)
    avg_stars = user.get("average_stars", 0.0)
    useful = user.get("useful", 0)
    return (user_id, [review_count, avg_stars, useful])

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
    user_features = user_dict.get(user_id, [0, 0.0, 0])
    business_features = business_dict.get(business_id, [0.0, 0, 0])
    return user_features + business_features

# ----------------- Hybrid 系统主流程 -----------------
def main(folder_path, test_file, output_file):
    spark = SparkSession.builder.appName("HybridRS").getOrCreate()
    sc = spark.sparkContext

    # ----------------- CF 部分: 基于评分构建数据结构 -----------------
    rdd = sc.textFile(os.path.join(folder_path, "yelp_train.csv"))
    header = rdd.first()
    train_rdd = rdd.filter(lambda x: x != header)
    # 构造 business_sets: 业务 -> {user: rating}
    business_rdd = train_rdd.map(lambda x: x.split(',')).map(lambda x: (x[1], (x[0], float(x[2]))))
    business_sets = business_rdd.groupByKey().mapValues(lambda vals: {u: r for u, r in vals}).collectAsMap()
    business_means = {b: sum(ratings.values()) / len(ratings) for b, ratings in business_sets.items()}
    # 构造 user_sets: 用户 -> {business: rating}
    user_rdd = train_rdd.map(lambda x: x.split(',')).map(lambda x: (x[0], (x[1], float(x[2]))))
    user_sets = user_rdd.groupByKey().mapValues(lambda vals: {b: r for b, r in vals}).collectAsMap()
    overall_avg = train_rdd.map(lambda x: float(x.split(',')[2])).mean()
    user_means = {u: sum(ratings.values()) / len(ratings) for u, ratings in user_sets.items()}

    broadcast_business = sc.broadcast(business_sets)
    broadcast_user = sc.broadcast(user_sets)
    broadcast_business_means = sc.broadcast(business_means)
    broadcast_user_means = sc.broadcast(user_means)
    broadcast_overall = sc.broadcast(overall_avg)

    # ----------------- Model-Based 部分: 构造特征并训练回归模型 -----------------
    user_rdd_json = sc.textFile(os.path.join(folder_path, "user.json"))
    business_rdd_json = sc.textFile(os.path.join(folder_path, "business.json"))
    # 提取训练数据中的用户和业务 ID
    train_ids = train_rdd.map(lambda x: x.split(',')).cache()
    user_ids_train = set(train_ids.map(lambda x: x[0]).distinct().collect())
    business_ids_train = set(train_ids.map(lambda x: x[1]).distinct().collect())
    user_features_rdd = user_rdd_json.filter(lambda line: json.loads(line)["user_id"] in user_ids_train).map(parse_user_json)
    business_features_rdd = business_rdd_json.filter(lambda line: json.loads(line)["business_id"] in business_ids_train).map(parse_business_json)
    user_features_dict = dict(user_features_rdd.collect())
    business_features_dict = dict(business_features_rdd.collect())
    # 构造训练特征及标签
    training_data = train_ids.map(lambda x: (x[0], x[1], float(x[2]))).map(
        lambda x: (build_feature_vector(x[0], x[1], user_features_dict, business_features_dict), x[2])
    ).collect()
    X_train = np.array([item[0] for item in training_data])
    y_train = np.array([item[1] for item in training_data])
    model = XGBRegressor(max_depth=10, learning_rate=0.1, n_estimators=100, objective='reg:linear')
    model.fit(X_train, y_train)

    # ----------------- Hybrid 预测 -----------------
    test_rdd = sc.textFile(test_file)
    test_header = test_rdd.first()
    test_data = test_rdd.filter(lambda x: x != test_header).map(lambda x: x.split(',')).map(
        lambda x: (x[0], x[1])
    ).collect()

    # # 设定参数
    neighbor_limit = 50
    # K = 100.0  # 用于衡量业务评价数量的超参数

    all_neighbor_counts = [len(ratings) for ratings in broadcast_business.value.values()]
    neighbor_limit_auto = np.percentile(all_neighbor_counts, 95)
    
    # 计算所有业务的评论数的中位数作为 K
    all_review_counts = [features[1] for features in business_features_dict.values()]
    K_auto = np.percentile(all_review_counts, 10)

    predictions = []
    for user_id, business_id in test_data:
        # 1. CF 预测及邻居数量
        cf_pred, neighbor_count = predict_rating_for_pair_cf(
            user_id, business_id,
            broadcast_business.value,
            broadcast_user.value,
            broadcast_business_means.value,
            broadcast_user_means.value,
            broadcast_overall.value,
            neighbor_limit=10  
        )
        # 2. 模型预测：构造特征向量后预测评分
        features = build_feature_vector(user_id, business_id, user_features_dict, business_features_dict)
        features = np.array(features).reshape(1, -1)
        model_pred = model.predict(features)[0]
        model_pred = max(1.0, min(5.0, model_pred))
        # 3. 动态计算混合权重 α
        # 利用 CF 邻居数量越少、业务评价数量越高时降低 CF 的权重
        if business_id in business_features_dict:
            review_count = business_features_dict[business_id][1]  # 业务的评论数
        else:
            review_count = 0
        # 先以邻居数量归一化，再根据评论数调节权重（评论数越高，model-based权重越大，即 α 越低）
        if neighbor_count == 0 and review_count == 0:
            alpha = 1.0
        else:
            alpha = (min(neighbor_count, neighbor_limit_auto) / neighbor_limit_auto) * (K_auto / (review_count + K_auto))

        # alpha = 0.5
        # 4. 加权平均得到最终预测评分
        final_pred = alpha * cf_pred + (1 - alpha) * model_pred
        predictions.append((user_id, business_id, final_pred))

    # 输出结果
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("user_id,business_id,prediction\n")
        for user_id, business_id, pred in predictions:
            f.write(f"{user_id},{business_id},{pred}\n")

    spark.stop()

if __name__ == "__main__":
    folder_path = sys.argv[1]
    test_file = sys.argv[2]
    output_file = sys.argv[3]
    main(folder_path, test_file, output_file)


