from pyspark.sql import SparkSession
import sys
import json
from collections import defaultdict
from itertools import combinations
from time import time



def apriori(baskets: list, support: float) -> list:
    # for one item
    item_count = defaultdict(int)
    for basket in baskets:
        for item in list(basket):
            item_count[item] += 1


    L1 = {frozenset([item]) for item, count in item_count.items() if count >= support}

    frequent_itemset = list(L1)

    # for items
    k = 2  
    current_Lk = L1
    while current_Lk:
    # flatten the set of items
        unique_items = set()
        for itemset in current_Lk:
            for item in itemset:
                unique_items.add(item)
        # Generate candidate itemsets of size k
        Ck = {
            frozenset(candidate) for candidate in combinations(unique_items, k)
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
    partition_list = list(partition)
    if not partition_list:
        return iter([])

    support_by_partition = len(partition_list) / total_num * support

    for itemset in apriori(partition_list, support_by_partition):
        yield frozenset(itemset)





def output_format(candidates):
    itemset_groups = {}
    for itemset in candidates:
        size = len(itemset)
        if size not in itemset_groups:
            itemset_groups[size] = []
        # **按照整数值排序**
        itemset_groups[size].append(tuple(sorted(itemset, key=int)))  

    # **对所有项集按照 (长度, itemset) 进行排序**
    sorted_keys = sorted(itemset_groups.keys())
    
    output_lines = []
    for k in sorted_keys:
        sorted_itemsets = sorted(itemset_groups[k], key=lambda x: (len(x), x))  # **整体排序**
        formatted_group = ','.join(
            f"('{str(itemset[0])}')" if len(itemset) == 1 else str(itemset)
            for itemset in sorted_itemsets
        )
        output_lines.append(formatted_group)
        output_lines.append('') 
    
    return '\n'.join(output_lines)




def preprocessing(input_file_path):
    spark = SparkSession.builder.appName('task2').getOrCreate()
    sc = spark.sparkContext

    raw_rdd = sc.textFile(input_file_path)

    header = raw_rdd.first()

    preprocess_rdd = raw_rdd.filter(lambda x: x != header).map(lambda x: x.split(","))

    merged_rdd = preprocess_rdd.map(lambda x: (
        str(x[0].strip('"')[:-4] + x[0].strip('"')[-2:] + "-" + x[1].strip('"')), 
        int(x[5].strip('"')) 
    ))

    collected_data = merged_rdd.collect()

    # Write to a local CSV file
    with open("customer_product.csv", "w") as f:
        # Write header
        f.write("DATE-CUSTOMER_ID,PRODUCT_ID\n")

        for row in collected_data:
            f.write(f"{str(row[0])},{int(row[1])}\n")


def main(filter_threshold, support, input_file_path, output_file_path):
    preprocessing(input_file_path)

    spark = SparkSession.builder.appName('task2').getOrCreate()
    sc = spark.sparkContext
    
    start = time()

    raw_rdd = sc.textFile("./customer_product.csv")

    header = raw_rdd.first()
    
    rdd_kv = raw_rdd.filter(lambda x: x != header).map(lambda x: x.split(',')) 

    # print(f"Total number of purchase: {rdd_kv.count()}")

    basket = rdd_kv.map(lambda x: (x[0], {x[1]})).reduceByKey(lambda a, b: a | b).map(lambda x: x[1]).filter(lambda x: len(x) > filter_threshold)

    # with open("basket.csv", "w") as f:

    #     for row in basket.collect():
    #         f.write(f"{row}\n")

    total_num = basket.count()

    print(f"DEBUG: Total Baskets -> {total_num}")


    # partition_1 = basket.mapPartitionsWithIndex(lambda idx, it: it if idx == 0 else [])

    # with open("basket.csv", "w") as f:

    #     for row in partition_1.collect():
    #         f.write(f"{row}\n")

    # bf_candidates_1 = partition_1.mapPartitions(lambda partition: [(frozenset(itemset), 1) for itemset in SON1_by_partition(partition, total_num, support)]).flatMap(lambda x: x) 


    # with open("bf_candidates_1.csv", "w") as f:

    #     for row in bf_candidates_1.collect():
    #         f.write(f"{row}\n")


    candidates = basket.mapPartitions(
        lambda partition: ((frozenset(itemset), 1) for itemset in SON1_by_partition(partition, total_num, support))
    ).distinct()

    candidates_list = candidates.map(lambda x: x[0]).collect()

    intermediate_results = output_format(candidates_list)
   
    def count_candidates(partition):
        local_counts = defaultdict(int)

        for basket in partition:  # 直接遍历 iterator，避免 OOM
            for cand in candidates_list:
                if cand.issubset(basket):  
                    local_counts[cand] += 1  

        return local_counts.items()

    phase2_counts = basket.mapPartitions(count_candidates)

    final_counts = phase2_counts.reduceByKey(lambda x, y: x + y).filter(lambda x: x[1] >= support)

    final_results = output_format(final_counts.map(lambda x: x[0]).collect())
   
    with open(output_file_path, 'w') as f:
        f.write("Candidates:\n")
        f.write(intermediate_results)
        f.write('\n')
        f.write("Frequent Itemsets:\n")
        f.write(final_results)
        # f.write('\n')
        #     # Format final_counts correctly before writing
        # f.write("Final Counts (support values):\n")
        # f.write('\n'.join([f"{itemset}: {count}" for itemset, count in final_counts.collect()]))
        # f.write('\n')


    end = time()
    print(f'Duration: {end - start}')

if __name__ == "__main__":
    filter_threshold = int(sys.argv[1])
    support = int(sys.argv[2])
    input_filepath = sys.argv[3]
    output_filepath = sys.argv[4]

    main(filter_threshold, support, input_filepath, output_filepath)


