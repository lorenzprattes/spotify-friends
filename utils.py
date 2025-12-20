import json
import networkx as nx


def load_graph(path: str) -> nx.DiGraph:
    """Load a scraped JSONL file into a NetworkX directed graph.
    
    Edges represent follower relationships: follower -> followed user.
    
    Node attributes:
        - name: Display name
        - followers_count: Number of followers
    
    Args:
        path: Path to the JSONL file.
        
    Returns:
        A directed graph with user IDs as nodes.
    """
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
