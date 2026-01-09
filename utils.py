import json
import networkx as nx
import rustworkx as rx


def load_graph(path: str) -> nx.DiGraph:

    G = nx.DiGraph()
    
    with open(path) as f:
        for line in f:
            record = json.loads(line)
            
            if "error" in record:
                continue
            
            user_id = record["id"]
            profiles = record.get("profiles", [])
            
            # Build lookup from user ID to profile data
            profile_map = {}
            for p in profiles:
                uri = p.get("uri", "")
                if uri.startswith("spotify:user:"):
                    pid = uri.split(":")[-1]
                    profile_map[pid] = p
            
            G.add_node(user_id)
            
            for follower_id, profile in profile_map.items():
                G.add_node(
                    follower_id,
                    name=profile.get("name"),
                    followers_count=profile.get("followers_count"),
                )
                G.add_edge(follower_id, user_id)
    
    return G


def load_graph_v2(path: str) -> nx.DiGraph:
    G = nx.DiGraph()
    error_count = 0
    with open(path) as f:

        for line in f:
            record = json.loads(line)
            
            if "error" in record:
                error_count =+ 1
                continue
            
            user_id = record["id"]
            name = record["name"]
            follower_count = record["followers_count"]
            followers = record["follower_list"]

            G.add_node(user_id)
            
            for follower_id in followers:
                G.add_node(
                    follower_id
                )
                G.add_edge(follower_id, user_id)
    
    return G

def load_graph_v3(path: str) -> nx.DiGraph:
    G = nx.DiGraph()
    error_count = 0
    with open(path) as f:

        for line in f:
            record = json.loads(line)
            
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

    return G


def load_graph_v3_rustworkx(path: str) -> tuple[rx.PyDiGraph, dict]:
    G = rx.PyDiGraph()
    id_to_index = {}
    error_count = 0
    
    with open(path) as f:
        for line in f:
            record = json.loads(line)
            
            if "error" in record:
                error_count += 1
                continue
            
            user_id = record["id"]
            name = record["name"]
            follower_count = record["followers_count"]
            profiles = record.get("follower_profiles", [])
            
            # Add or update the main user node
            if user_id not in id_to_index:
                idx = G.add_node({
                    "id": user_id,
                    "name": name,
                    "followers_count": follower_count
                })
                id_to_index[user_id] = idx
            else:
                idx = id_to_index[user_id]
                G[idx] = {
                    "id": user_id,
                    "name": name,
                    "followers_count": follower_count
                }
            
            # Add follower nodes and edges
            for follower_id, follower_name, follower_follower_count in profiles:
                if follower_id not in id_to_index:
                    follower_idx = G.add_node({
                        "id": follower_id,
                        "name": follower_name,
                        "followers_count": follower_follower_count
                    })
                    id_to_index[follower_id] = follower_idx
                else:
                    follower_idx = id_to_index[follower_id]
                    # Update node data if we have more info
                    if follower_name:
                        G[follower_idx] = {
                            "id": follower_id,
                            "name": follower_name,
                            "followers_count": follower_follower_count
                        }
                
                # Add edge from follower to user
                G.add_edge(follower_idx, idx, None)
    
    return G, id_to_index