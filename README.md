# spotify-friends

Scrape Spotify user follower networks and visualize them as graphs.

## Installation

This project uses uv, a all in one solution for python. Install [here](https://docs.astral.sh/uv/) or with `curl -LsSf https://astral.sh/uv/install.sh | sh`.

Afterwards, run this in inside the project folder:
```bash
uv sync
uv run playwright install chromium
```

## Usage

```bash
uv run python run_scraper.py <username> [depth] [max_followers] [output_file]
```

Example:
```bash
uv run python run_scraper.py l0renzz 2 100 output.jsonl
```

Arguments:
- `username` - Spotify username to start from
- `depth` - How many levels of followers to crawl (default: 2)
- `max_followers` - Max followers to fetch per user (default: 100)
- `output_file` - Output file path (default: `spotify_graph_<user>_<depth>.jsonl`)

## Output

Results are saved as JSONL with one JSON object per line:

```json
{
  "id": "username",
  "depth": 1,
  "followers_count": 42,
  "follower_list": ["user1", "user2", ...],
  "profiles": [...] //uris of the users, not strictly needed, eg "spotify:user:<username>"
}
```

Use `explore_network.ipynb` to visualize the graph. (TODO: Build import logic to read a networkX network from the json files)