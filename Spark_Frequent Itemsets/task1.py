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

    support_by_partition = len(partition_list)/total_num * support

    result = apriori(partition_list, support_by_partition)

    print(f"Partition processed: {len(partition_list)} baskets, returned {len(result)} candidates")
    return result


def output_format(candidates):
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





def main(case_id, support, input_filepath, output_filepath):

    start = time()
    
    spark = SparkSession.builder.appName('task1').getOrCreate()
    sc = spark.sparkContext

    # read input file

    rdd = sc.textFile(input_filepath)
    header = rdd.first()
    rdd_kv = rdd.filter(lambda x: x != header).map(lambda x: x.split(',')) 


    if case_id == "1":
        basket = rdd_kv.map(lambda x: (x[0], {x[1]})).reduceByKey(lambda a, b: a | b).map(lambda x: x[1])

        # with open("basket_task1.csv", "w") as f:

        #     for row in basket.collect():
        #         f.write(f"{row}\n")

    elif case_id == "2":
        basket = rdd_kv.map(lambda x: (x[1], {x[0]})).reduceByKey(lambda a, b: a | b).map(lambda x: x[1])

    else:
        print("Invalid case_id")
        return


        # phrase 1
    # basket = rdd.map(lambda x: (x[0], x[1])).groupByKey().map(lambda x: set(x[1]))
    total_num = basket.count()

    candidates = basket.mapPartitions(lambda partition: [(frozenset(itemset), 1) for itemset in SON1_by_partition(partition, total_num, support)]) \
        .reduceByKey(lambda x, y: x + y).map(lambda x: x[0])
    candidates_list = candidates.collect()

    intermediate_results = output_format(candidates_list)

    # phrase 2
    def count_candidates(partition):
        baskets = list(partition)  # Convert partition to a list
        return [(cand, sum(1 for basket in baskets if cand.issubset(basket))) for cand in candidates_list]

    phase2_counts = basket.mapPartitions(count_candidates)

    final_counts = phase2_counts.reduceByKey(lambda x, y: x + y).filter(lambda x: x[1] >= support)

    final_results = output_format(final_counts.map(lambda x: x[0]).collect())




    with open(output_filepath, 'w') as f:
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
    case_id = sys.argv[1]
    support = int(sys.argv[2])
    input_filepath = sys.argv[3]
    output_filepath = sys.argv[4]

    main(case_id, support, input_filepath, output_filepath)
