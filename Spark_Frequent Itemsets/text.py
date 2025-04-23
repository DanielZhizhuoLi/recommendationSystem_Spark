from pyspark.sql import SparkSession
import sys
import json
from collections import defaultdict
from itertools import combinations
from time import time


def apriori(baskets: list, support: float) -> list:
    """Apriori algorithm to find frequent itemsets."""
    item_count = defaultdict(int)

    for basket in baskets:
        for item in list(basket):
            item_count[item] += 1

    L1 = {frozenset([item]) for item, count in item_count.items() if count >= support}

    frequent_itemset = list(L1)

    # Iteratively generate larger frequent itemsets
    k = 2
    current_Lk = L1
    while current_Lk:
        unique_items = set()
        for itemset in current_Lk:
            unique_items.update(itemset)

        Ck = {
            frozenset(candidate)
            for candidate in combinations(unique_items, k)
            if all(frozenset(subset) in current_Lk for subset in combinations(candidate, k - 1))
        }

        itemset_count = defaultdict(int)
        for basket in baskets:
            for itemset in Ck:
                if itemset.issubset(basket):
                    itemset_count[itemset] += 1

        Lk = {itemset for itemset, count in itemset_count.items() if count >= support}

        if not Lk:
            break

        frequent_itemset.extend(Lk)
        current_Lk = Lk
        k += 1

    return [set(itemset) for itemset in frequent_itemset]


def SON1_by_partition(partition, total_num, support):
    """Phase 1 of SON Algorithm: Apriori applied per partition."""
    partition_list = list(partition)
    if not partition_list:
        return iter([])

    support_by_partition = len(partition_list) / total_num * support

    for itemset in apriori(partition_list, support_by_partition):
        yield frozenset(itemset)


def output_format(candidates):
    """Format output for candidates and frequent itemsets."""
    itemset_groups = {}
    for itemset in candidates:
        size = len(itemset)
        if size not in itemset_groups:
            itemset_groups[size] = []
        itemset_groups[size].append(tuple(sorted(itemset)))

    sorted_keys = sorted(itemset_groups.keys())
    
    output_lines = []
    for k in sorted_keys:
        formatted_group = ','.join(
            f"('{str(itemset[0])}')" if len(itemset) == 1 else str(itemset)  
            for itemset in sorted(itemset_groups[k])
        )
        output_lines.append(formatted_group)
        output_lines.append('')  # Empty line after each group
    
    return '\n'.join(output_lines)


def main(filter_threshold, support, output_file_path):
    """Main function for SON Algorithm."""
    spark = SparkSession.builder.appName("task2").getOrCreate()
    sc = spark.sparkContext
    
    start = time()
                          
    # Read and preprocess data
    raw_rdd = sc.textFile("./customer_product.csv")
    header = raw_rdd.first()
    
    rdd_kv = raw_rdd.filter(lambda x: x != header).map(lambda x: x.split(','))

    basket = rdd_kv.map(lambda x: (x[0], {x[1]})).reduceByKey(lambda a, b: a | b).map(lambda x: x[1])
    basket = basket.filter(lambda x: len(x) > filter_threshold).persist()

    total_num = basket.count()
    print(f"DEBUG: Total Baskets -> {total_num}")

    # SON Phase 1: Generate Candidates
    bf_candidates = basket.mapPartitions(
        lambda partition: ((frozenset(itemset), 1) for itemset in SON1_by_partition(partition, total_num, support))
    ).distinct()

    candidates_list = bf_candidates.map(lambda x: x[0]).collect()

    intermediate_results = output_format(candidates_list)

    # SON Phase 2: Count Candidates
    def count_candidates(partition):
        baskets = list(partition)
        local_counts = defaultdict(int)

        for basket in baskets:
            for cand in candidates_list:
                if cand.issubset(basket):
                    local_counts[cand] += 1  

        return local_counts.items()

    phase2_counts = basket.mapPartitions(count_candidates)

    final_counts = phase2_counts.reduceByKey(lambda x, y: x + y).filter(lambda x: x[1] >= support)

    final_results = output_format(final_counts.map(lambda x: x[0]).collect())

    # Write output file
    with open(output_file_path, "w") as f:
        f.write("Candidates:\n")
        f.write(intermediate_results)
        f.write("\nFrequent Itemsets:\n")
        f.write(final_results)

    end = time()
    print(f"Duration: {end - start} seconds")


if __name__ == "__main__":
    filter_threshold = int(sys.argv[1])
    support = int(sys.argv[2])
    output_filepath = sys.argv[3]

    main(filter_threshold, support, output_filepath)
