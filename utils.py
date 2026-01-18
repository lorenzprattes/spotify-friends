import json
import networkx as nx
import rustworkx as rx

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
                    if follower_name:
                        G[follower_idx] = {
                            "id": follower_id,
                            "name": follower_name,
                            "followers_count": follower_follower_count
                        }
                
                # Add edges,  ut no duplicates
                if not G.has_edge(follower_idx, idx):
                    G.add_edge(follower_idx, idx, None)
    
    return G, id_to_index


def anonymize_dataset_to_file(input_path: str, output_path: str):
    """
    Reads the original JSONL dataset, replaces string user IDs with ascending integers,
    removes PII (names), and exports to a new JSONL file.
    
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

    # error_count = 0
    with open(input_path, 'r') as f_in, open(output_path, 'w') as f_out:
        for line in f_in:
            try:
                record = json.loads(line)
                
                # specific validation for our dataset
                if "error" in record:
                    continue
                
                # Anonymize the main user
                original_uid = record["id"]
                anon_uid = get_id(original_uid)
                followers_count = record["followers_count"]
                
                # Process followers
                anon_profiles = []
                # Expects structure: [id, name, count]
                for fol_id, _, fol_count in record.get("follower_profiles", []):
                    anon_fol_id = get_id(fol_id)
                    # We keep the structure [id, name, count] but name is None to match existing loaders
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




import re

def _sanitize_xml_string(s):
    """Remove characters that are illegal in XML."""
    if s is None:
        return ""
    if not isinstance(s, str):
        return s
    # Remove NULL and other illegal XML 1.0 characters
    # XML 1.0 legal characters: #x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD]
    result = ""
    for char in s:
        codepoint = ord(char)
        if codepoint == 0x9 or codepoint == 0xA or codepoint == 0xD:
            result += char
        elif 0x20 <= codepoint <= 0xD7FF:
            result += char
        elif 0xE000 <= codepoint <= 0xFFFD:
            result += char
        # Skip everything else (including NULL, control chars, surrogates, etc.)
    return result


def export_to_gephi(input_path: str, output_path: str, graph_format: str = "gexf"):
    """Export a JSONL graph file to a Gephi-compatible format.
    
    Args:
        input_path: Path to the input JSONL file
        output_path: Path for the output file (extension will be added if needed)
        graph_format: Format to export ('gexf', 'graphml', 'gml'). Default: 'gexf'
    
    Supported formats:
        - gexf: Graph Exchange XML Format (recommended for Gephi)
        - graphml: GraphML format
        - gml: Graph Modeling Language
    """
    # Load the graph
    G = load_graph_v3(input_path)
    
    # Clean None values and illegal XML characters from node attributes
    for node in G.nodes():
        attrs = G.nodes[node]
        for key, value in list(attrs.items()):
            if value is None:
                attrs[key] = ""  # Replace None with empty string
            elif isinstance(value, str):
                attrs[key] = _sanitize_xml_string(value)
            elif isinstance(value, (int, float)) and value != value:  # Check for NaN
                attrs[key] = 0
    
    # Create a new graph with sanitized node IDs
    G_clean = nx.DiGraph()
    node_mapping = {}
    for node in G.nodes():
        clean_node = _sanitize_xml_string(str(node)) if isinstance(node, str) else node
        node_mapping[node] = clean_node
        G_clean.add_node(clean_node, **G.nodes[node])
    
    for u, v in G.edges():
        G_clean.add_edge(node_mapping[u], node_mapping[v])
    
    # Ensure output path has correct extension
    if not output_path.endswith(f".{graph_format}"):
        output_path = f"{output_path}.{graph_format}"
    
    # Export based on format
    if graph_format == "gexf":
        nx.write_gexf(G_clean, output_path)
    elif graph_format == "graphml":
        nx.write_graphml(G_clean, output_path)
    elif graph_format == "gml":
        nx.write_gml(G_clean, output_path)
    else:
        raise ValueError(f"Unsupported format: {graph_format}. Use 'gexf', 'graphml', or 'gml'")
    
    # Post-process: remove any remaining NULL characters from the file
    with open(output_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Remove NULL and other problematic characters
    clean_content = content.replace('\x00', '')
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(clean_content)
    
    print(f"Graph exported to {output_path}")
    print(f"Nodes: {G_clean.number_of_nodes()}, Edges: {G_clean.number_of_edges()}")

