import networkx as nx
import random
import numpy as np
import networkit as nk
import json


def approx_average_shortest_path_length_nk(
    graph: nk.Graph, 
    num_samples: int = 100, 
    seed: int | None = None
):
    """
    Estimates average path length and standard deviation using NetworKit.

    From https://i11www.iti.kit.edu/_media/projects/spp1126/files/sw-acct-05.pdf
    
    Args:
        graph: A NetworKit graph
        num_samples (int): Number of source nodes to sample.

    Returns:
        dict: {'mean': float, 'std_dev': float, 'sample_size': int}
    """
    if seed:
        random.seed(seed)

    shortest_paths_dijkstra = []
    sample_nodes = random.sample(range(graph.numberOfNodes()), num_samples)

    for node in sample_nodes:
        bfs = nk.distance.BFS(graph, node).run()
        dists = bfs.getDistances()
        shortest_paths_dijkstra.extend(dists)
    
    avg_path_length = np.mean(shortest_paths_dijkstra)
    std_dev = np.std(shortest_paths_dijkstra)
    return {
        'mean': avg_path_length,
        'std_dev': std_dev,
        'count': len(shortest_paths_dijkstra)
    }

def calculate_avg_clustering_coefficient_nk(
    graph: nk.Graph
):
    """
    Calculates the avg clustering coefficient, as builtin tools from NetworKit
    dont include nodes with a degree lower then 2 in their calculation (i think they use triangles to calculate the values and 0 and 1 degree nodes cant form those)

    Args:
        graph: A NetworKit Graph

    Returns:
        float
    """
    local_clustering = nk.centrality.LocalClusteringCoefficient(graph, turbo=True)
    local_clustering.run()
    clustering_coeffs = local_clustering.scores()
    avg_clustering_coeff = np.mean(clustering_coeffs)
    return avg_clustering_coeff
        
def load_graph_v3(path: str) -> nx.DiGraph:
    G = nx.DiGraph()
    error_count = 0
    line_count = 0
    with open(path) as f:
        for line in f:
            record = json.loads(line)
            line_count += 1
            if "error" in record:
                error_count += 1
                continue
            
            user_id = record["id"]
            name = record["name"]
            follower_count = record["followers_count"]
            profiles = record.get("follower_profiles", [])

            G.add_node(user_id, name=name, followers_count=follower_count)
            
            for follower_id, name, follower_follower_count in profiles:
                G.add_node(
                    follower_id,
                    name=name,
                    followers_count=follower_follower_count,
                )
                G.add_edge(follower_id, user_id)
    print("Lines read: ", line_count)
    print("Errors found : ", error_count)
    return G



def anonymize_dataset_to_file(input_path: str, output_path: str):
    """
    Reads original json dataset and ananonymizes with ascending user ids. 
    
    The structure is compatible with the existing load_graph_v3 functions.
    output format:
    {
        "id": int,                 # Anonymized Node ID (0 to N)
        "followers_count": int,    # Preserved metric
        "follower_profiles": [     # List of followers
            [int, null, int],      # [Anonymized Follower ID, name (removed), Follower Count]
            ...
        ]
    }
    """
    mapping = {}
    next_id = 0
    
    def get_id(original_id):
        nonlocal next_id
        if original_id not in mapping:
            mapping[original_id] = next_id
            next_id += 1
        return mapping[original_id]

    error_count = 0
    with open(input_path, 'r') as f_in, open(output_path, 'w') as f_out:
        for line in f_in:
            try:
                record = json.loads(line)
                
                if "error" in record:
                    continue
                
                original_uid = record["id"]
                anon_uid = get_id(original_uid)
                followers_count = record["followers_count"]

                anon_profiles = []
                for fol_id, _, fol_count in record.get("follower_profiles", []):
                    anon_fol_id = get_id(fol_id)
                    #  keep the structure [id, name, count] but name is None to keep loaders
                    anon_profiles.append([anon_fol_id, None, fol_count])
                
                new_record = {
                    "id": anon_uid,
                    "name": None, # Explicitly remove name
                    "followers_count": followers_count,
                    "follower_profiles": anon_profiles
                }
                
                f_out.write(json.dumps(new_record) + "\n")
                
            except json.JSONDecodeError:
                continue

    print(f"Exported anonymized dataset to {output_path}")
    print(f"Total unique nodes: {len(mapping)}")

