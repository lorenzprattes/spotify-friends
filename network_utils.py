import networkx as nx
import rustworkx as rx
import random
import numpy as np
import networkit as nk

def analyze_path_lengths_rx(
    graph: rx.PyGraph | rx.PyDiGraph, 
    num_samples: int = 100,
    seed: int | None = None
):
    """
    Estimates average path length and standard deviation using rustworkx.

    From https://i11www.iti.kit.edu/_media/projects/spp1126/files/sw-acct-05.pdf I think
    
    Args:
        graph: A rustworkx PyGraph (undirected) or PyDiGraph (directed).
        num_samples (int): Number of source nodes to sample.
        weight_fn (callable, optional): Function to extract edge weights. 
                                        Defaults to unweighted (distance=1.0).
    
    Returns:
        dict: {'mean': float, 'std_dev': float, 'sample_size': int}
    """
    if seed is not None:
        random.seed(seed)
    node_indices = list(graph.node_indices())
    
    k = min(num_samples, len(node_indices))
    sampled_sources = random.sample(node_indices, k)
    
    weight_fn = lambda _: 1.0
    collected_paths = []

    for source in sampled_sources:
        paths = rx.dijkstra_shortest_path_lengths(graph, source, weight_fn)
        
        valid_lengths = [
            dist for target, dist in paths.items() 
            if target != source
        ]
        collected_paths.extend(valid_lengths)

    if not collected_paths:
        return {'mean': 0.0, 'std_dev': 0.0, 'sample_size': 0}

    avg_len = np.mean(collected_paths)
    std_dev = np.std(collected_paths)
    
    return {
        'mean': avg_len,
        'std_dev': std_dev,
        'count': len(collected_paths)
    }


def approx_average_shortest_path_length_nk(
    graph: nk.Graph, 
    num_samples: int = 100, 
    seed: int | None = None
):
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
    local_clustering = nk.centrality.LocalClusteringCoefficient(graph, turbo=True)
    local_clustering.run()
    clustering_coeffs = local_clustering.scores()
    avg_clustering_coeff = np.mean(clustering_coeffs)
    return {
        'mean': avg_clustering_coeff,
        'std_dev': np.std(clustering_coeffs),
        'count': len(clustering_coeffs)
    }