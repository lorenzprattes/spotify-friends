# spotify-friends

This project scrapes the spotify "/followers" endpoint to build a network of users.
We generate the necessary tokens for interaction with the endpoint by using [scrapy-playwright](https://github.com/scrapy-plugins/scrapy-playwright) and scrape the endpoint with [scrapy](https://www.scrapy.org/).

Disclaimer: This scraper is highly dependent on spotifys current rate limiting and bot detection practices. You will have to change the scrapy settings, especially the timing, and the scraper can run into dead ends. This can be caused by token starvation, or by simply getting blocked. In case of a scrape stopping, you have the option to resume, as lined out below.   
__YMMV!__

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

### Resume from checkpoint

```bash
uv run python run_scraper.py --resume checkpoint_<username>.json
```

Checkpoints are saved to `checkpoint_<username>.json`.

## Output

Results are saved as JSONL with one JSON object per line, for example:

```json
{
  "id": <id>,
  "name": <username>
  "depth": <x>,
  "followers_count": <x>,
  "profiles": [[<follower_id>, <follower_username>, <follower_count>]...] //contains uris of the users, and user details, this is needed due to scraper design and the graph construction processs
}
```

Use `explore_network.ipynb` for an analysis of the graph.

