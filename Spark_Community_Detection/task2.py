from pyspark.sql import SparkSession
import json
import sys
from itertools import combinations
import math
from collections import deque




def compute_edge_betweenness_single(graph, root):

    # BFS 
    levels = {}    
    sigma = {}      
    parents = {}     
    levels[root] = 0
    sigma[root] = 1
    parents[root] = []
    
    queue = deque([root])
    order = [] 

    while queue:
        current = queue.popleft()
        order.append(current)
        for nbr in graph.get(current, []):
            if nbr not in levels:
                levels[nbr] = levels[current] + 1
                queue.append(nbr)
                sigma[nbr] = 0
                parents[nbr] = []
            if levels[nbr] == levels[current] + 1:
                sigma[nbr] += sigma[current]
                parents[nbr].append(current)


    dependency = {node: 0.0 for node in levels}
    edge_credit = {}

    for node in reversed(order):
        for p in parents[node]:
            
            credit = (sigma[p] / sigma[node]) * (1 + dependency[node])
        
            edge = tuple(sorted((p, node)))
            edge_credit[edge] = edge_credit.get(edge, 0.0) + credit
            dependency[p] += credit

    return edge_credit




def find_communities(current_graph):

#Given the current graph (a dict: node -> set(neighbors)),

    visited = set()
    communities = []
    for node in current_graph.keys():
        if node not in visited:
            component = []
            stack = [node]
            visited.add(node)
            while stack:
                current = stack.pop()
                component.append(current)
                for nbr in current_graph.get(current, set()):
                    if nbr not in visited:
                        visited.add(nbr)
                        stack.append(nbr)
            communities.append(sorted(component))
    communities.sort(key=lambda comp: comp[0])
    return communities


def compute_modularity(communities, current_graph, original_degrees, m):

# For node pair (i,j) in the same community:
# Q += [A_ij - (k_i*k_j)/(2*m)]


    Q = 0.0
    for comm in communities:
        for i in range(len(comm)):
            for j in range(i+1, len(comm)):
                node_i = comm[i]
                node_j = comm[j]
                A_ij = 1 if node_j in current_graph.get(node_i, set()) else 0
                # k_i the original degree
                ki = original_degrees[node_i]
                kj = original_degrees[node_j]
                Q += (A_ij - (ki * kj) / (2.0 * m))
    return Q / (2.0 * m)




def main(threshold, input_file, betweenness_file, community_output):

    spark = SparkSession.builder.appName("task2").getOrCreate()
    sc = spark.sparkContext

    data = sc.textFile(input_file)
    header = data.first()
    rdd = data.filter(lambda line: line != header).map(lambda line: line.split(',')).map(lambda row: (row[0], row[1]))
    #print("rdd.take(5):", rdd.take(5))

    #get user:{business1, buiness2}
    user_set = rdd.groupByKey().mapValues(lambda x: set(x))

    #print("user_set.take(5):", rdd.take(5))

    # get business1:user1
    business_user_rdd = user_set.flatMap(lambda x: [(b, x[0]) for b in x[1]])
    
    # get business1:[user1, user2, user3]
    business_grouped = business_user_rdd.groupByKey().mapValues(lambda users: list(users))
    # get (user1, user2) pairs
    user_pairs_rdd = business_grouped.flatMap(lambda x: [tuple(sorted(pair)) for pair in combinations(x[1], 2)])

    pairs_rdd = user_pairs_rdd.map(lambda pair: (pair, 1)).reduceByKey(lambda a, b: a + b).filter(lambda x: x[1] >= threshold).keys()
    
    # print(pairs_rdd.take(5))
    

    node_neighbor = pairs_rdd.flatMap(lambda x: [(x[0], [x[1]]), (x[1], [x[0]])]).reduceByKey(lambda a, b: a + b)

    # print(node_neighbor.take(5))

    graph_dict = node_neighbor.collectAsMap()
    bc_graph = sc.broadcast(graph_dict)


    nodes = list(graph_dict.keys())
    nodes_rdd = sc.parallelize(nodes)
    
    edge_betweenness_rdd = nodes_rdd.flatMap(lambda node: list(compute_edge_betweenness_single(bc_graph.value, node).items()))

    total_betweenness = edge_betweenness_rdd.reduceByKey(lambda a, b: a + b).mapValues(lambda credit: credit / 2.0)
    

    betweenness_result = total_betweenness.collect()
    betweenness_sorted = sorted(betweenness_result, key=lambda x: (-x[1], x[0][0], x[0][1]))


    with open(betweenness_file, "w", encoding="utf-8") as f:
        for edge, bet in betweenness_sorted:
            line = f"('{edge[0]}', '{edge[1]}'),{round(bet, 5)}\n"
            f.write(line)
        


# task2.2

    original_graph = {node: set(neighbors) for node, neighbors in graph_dict.items()}
   
    m = sum(len(neighbors) for neighbors in original_graph.values()) // 2

    original_degrees = {node: len(neighbors) for node, neighbors in original_graph.items()}


    current_graph = {node: set(neighbors) for node, neighbors in original_graph.items()}

    best_modularity = -float('inf')
    best_communities = None


    while True:
        # get a dict for 
        overall_bet = {}
        for node in list(current_graph.keys()):
            bet = compute_edge_betweenness_single(current_graph, node)
            for edge, val in bet.items():
                overall_bet[edge] = overall_bet.get(edge, 0.0) + val
        # Each edge is counted twice—divide by 2.
        for edge in overall_bet:
            overall_bet[edge] /= 2.0

        # If no edges remain, break.
        if not overall_bet:
            break

        max_bet = max(overall_bet.values())

        edges_to_remove = [edge for edge, val in overall_bet.items() if abs(val - max_bet) < 1e-6]

        for edge in edges_to_remove:
            n1, n2 = edge
            if n2 in current_graph.get(n1, set()):
                current_graph[n1].remove(n2)
            if n1 in current_graph.get(n2, set()):
                current_graph[n2].remove(n1)


        communities = find_communities(current_graph)
        # Compute the modularity
        modularity = compute_modularity(communities, current_graph, original_degrees, m)



        if modularity > best_modularity:
            best_modularity = modularity
            best_communities = communities


        total_edges = sum(len(neighbors) for neighbors in current_graph.values())
        if total_edges == 0:
            break
    
    for comm in best_communities:
        comm.sort()
    
    sorted_communities = sorted(best_communities, key=lambda comm: (len(comm), comm[0]))

    with open(community_output, "w", encoding="utf-8") as f:
        lines = []
        for community in sorted_communities:
            line = ",".join([f"'{user}'" for user in community])
            lines.append(line)
        f.write("\n".join(lines))





if __name__ == "__main__":
    threshold = int(sys.argv[1])
    input_file = sys.argv[2]
    betweenness_output = sys.argv[3]
    community_output = sys.argv[4]
    main(threshold, input_file, betweenness_output, community_output)