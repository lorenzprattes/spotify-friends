import json
import networkx as nx
import rustworkx as rx

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

