"""Bootstrap a Metabase dashboard for HackerNews RSS Pipeline Analytics."""

import sys
from os import getenv

import requests
from loguru import logger

METABASE_URL = getenv("METABASE_URL")
METABASE_API_KEY = getenv("METABASE_API_KEY")
DATABASE_NAME = getenv("METABASE_BIGQUERY_HACKERNEWS_DB", "bigquery-hackernews")
DASHBOARD_NAME = "HackerNews RSS Pipeline Analytics"

session = requests.Session()
session.headers.update({"Content-Type": "application/json", "x-api-key": METABASE_API_KEY})


def authenticate():
    if not METABASE_API_KEY:
        logger.error("METABASE_API_KEY environment variable is not set")
        sys.exit(1)
    logger.info("Using API key from METABASE_API_KEY")


def api_request(method: str, path: str, json=None) -> dict:
    resp = session.request(method, f"{METABASE_URL}{path}", json=json)
    resp.raise_for_status()
    return resp.json() if resp.text else {}


def find_database_id(name: str) -> int:
    databases = api_request("GET", "/api/database")
    for db in databases["data"]:
        if db["name"] == name:
            logger.info("Database '{}' found (id={})", name, db["id"])
            return db["id"]
    logger.error("Database '{}' not found in Metabase", name)
    sys.exit(1)


def find_existing_card(name: str) -> int | None:
    cards = api_request("GET", "/api/card")
    for card in cards:
        if card["name"] == name:
            return card["id"]
    return None


def find_existing_dashboard(name: str) -> int | None:
    dashboards = api_request("GET", "/api/dashboard")
    for dash in dashboards:
        if dash["name"] == name:
            return dash["id"]
    return None


CARDS_TO_DELETE = ["Articles by Hour of Day"]


def delete_card_by_name(name: str) -> None:
    card_id = find_existing_card(name)
    if card_id:
        api_request("DELETE", f"/api/card/{card_id}")
        logger.info("Removed stale card: '{}' (id={})", name, card_id)


def create_card(name: str, sql: str, display: str, database_id: int, viz_settings: dict | None = None) -> int:
    existing_id = find_existing_card(name)
    if existing_id:
        payload = {
            "display": display,
            "dataset_query": {
                "database": database_id,
                "type": "native",
                "native": {"query": sql},
            },
            "visualization_settings": viz_settings or {},
        }
        api_request("PUT", f"/api/card/{existing_id}", json=payload)
        logger.info("Updated card: '{}' (id={})", name, existing_id)
        return existing_id

    payload = {
        "name": name,
        "display": display,
        "dataset_query": {
            "database": database_id,
            "type": "native",
            "native": {"query": sql},
        },
        "visualization_settings": viz_settings or {},
    }
    card = api_request("POST", "/api/card", json=payload)
    logger.info("Created card: '{}' (id={})", name, card["id"])
    return card["id"]


def create_dashboard(name: str, description: str) -> int:
    existing_id = find_existing_dashboard(name)
    if existing_id:
        logger.info("Dashboard already exists: '{}' (id={})", name, existing_id)
        return existing_id

    dash = api_request("POST", "/api/dashboard", json={"name": name, "description": description})
    logger.info("Created dashboard: '{}' (id={})", name, dash["id"])
    return dash["id"]


def build_dashcard(card_id: int, row: int, col: int, size_x: int, size_y: int) -> dict:
    return {
        "id": -card_id,
        "card_id": card_id,
        "row": row,
        "col": col,
        "size_x": size_x,
        "size_y": size_y,
    }


def build_heading_dashcard(text: str, row: int, col: int, size_x: int = 24, size_y: int = 1) -> dict:
    return {
        "id": -(row * 100 + col + 1000),
        "card_id": None,
        "row": row,
        "col": col,
        "size_x": size_x,
        "size_y": size_y,
        "visualization_settings": {
            "virtual_card": {
                "name": None,
                "display": "heading",
                "visualization_settings": {},
                "dataset_query": {},
                "archived": False,
            },
            "text": text,
        },
    }


def define_cards() -> list[dict]:
    """Single source of truth for all 12 chart cards."""
    return [
        {
            "name": "Total Articles Ingested",
            "sql": "SELECT COUNT(*) AS total FROM hackernews_rss.dim_articles",
            "display": "scalar",
            "grid": (1, 0, 6, 4),
        },
        {
            "name": "Unique Contributors",
            "sql": "SELECT COUNT(DISTINCT username) AS total FROM hackernews_rss.dim_articles",
            "display": "scalar",
            "grid": (1, 6, 6, 4),
        },
        {
            "name": "Unique Domains Tracked",
            "sql": "SELECT COUNT(*) AS total FROM hackernews_rss.domain_stats_spark",
            "display": "scalar",
            "grid": (1, 12, 6, 4),
        },
        {
            "name": "Avg Articles per Domain",
            "sql": ("SELECT ROUND(AVG(article_count), 1) AS total FROM hackernews_rss.domain_stats_spark"),
            "display": "scalar",
            "grid": (1, 18, 6, 4),
        },
        {
            "name": "Top Domains by Article Count",
            "sql": (
                "SELECT domain, article_count"
                " FROM hackernews_rss.domain_stats_spark"
                " ORDER BY article_count DESC LIMIT 10"
            ),
            "display": "bar",
            "grid": (6, 0, 12, 8),
        },
        {
            "name": "Domain Diversity: Articles vs Unique Authors",
            "sql": (
                "SELECT domain, article_count, unique_authors"
                " FROM hackernews_rss.domain_stats_spark"
                " ORDER BY article_count DESC"
            ),
            "display": "scatter",
            "grid": (6, 12, 12, 8),
            "viz_settings": {
                "graph.dimensions": ["domain"],
                "graph.metrics": ["article_count", "unique_authors"],
            },
        },
        {
            "name": "Articles by Feed Source & Hour",
            "sql": (
                "SELECT TIMESTAMP_TRUNC(published_at, HOUR) AS hour_bucket,"
                " source_feed, COUNT(*) AS article_count"
                " FROM hackernews_rss.articles_spark"
                " GROUP BY hour_bucket, source_feed"
                " ORDER BY hour_bucket"
            ),
            "display": "bar",
            "grid": (15, 0, 12, 8),
            "viz_settings": {
                "graph.dimensions": ["hour_bucket", "source_feed"],
                "graph.metrics": ["article_count"],
                "stackable.stack_type": "stacked",
            },
        },
        {
            "name": "Feed Source Distribution",
            "sql": (
                "SELECT source_feed, COUNT(*) AS article_count"
                " FROM hackernews_rss.articles_spark"
                " GROUP BY source_feed ORDER BY article_count DESC"
            ),
            "display": "pie",
            "grid": (15, 12, 12, 8),
        },
        {
            "name": "Domain Activity Span (Hours)",
            "sql": (
                "SELECT domain,"
                " TIMESTAMP_DIFF(latest_published_at, earliest_published_at, HOUR)"
                " AS activity_span_hours"
                " FROM hackernews_rss.domain_stats_spark"
                " WHERE latest_published_at != earliest_published_at"
                " ORDER BY activity_span_hours DESC LIMIT 10"
            ),
            "display": "row",
            "grid": (23, 0, 12, 8),
        },
        {
            "name": "Top Contributors Across Feeds",
            "sql": (
                "SELECT username, source_feed, article_count,"
                " FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', earliest_published_at) AS first_post,"
                " FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', latest_published_at) AS last_post"
                " FROM hackernews_rss.top_posters_spark"
                " ORDER BY article_count DESC, username LIMIT 15"
            ),
            "display": "table",
            "grid": (23, 12, 12, 8),
        },
        {
            "name": "Author Contribution Ratio by Domain",
            "sql": (
                "SELECT domain,"
                " ROUND(SAFE_DIVIDE(unique_authors, article_count), 2) AS author_article_ratio"
                " FROM hackernews_rss.domain_stats_spark"
                " WHERE article_count >= 2"
                " ORDER BY author_article_ratio DESC"
            ),
            "display": "bar",
            "grid": (32, 0, 12, 8),
        },
        {
            "name": "Recent Articles Detail",
            "sql": (
                "SELECT title, username,"
                " REGEXP_EXTRACT(url, r'^https?://([^/]+)') AS domain,"
                " FORMAT_TIMESTAMP('%m-%d %H:%M', published_at) AS published,"
                " source_feed"
                " FROM hackernews_rss.articles_spark"
                " ORDER BY published_at DESC LIMIT 20"
            ),
            "display": "table",
            "grid": (32, 12, 12, 8),
        },
    ]


def define_headings() -> list[dict]:
    """Section headings for the dashboard."""
    return [
        {"text": "Pipeline KPIs", "grid": (0, 0, 24, 1)},
        {"text": "Domain Analysis", "grid": (5, 0, 24, 1)},
        {"text": "Publishing Activity", "grid": (14, 0, 24, 1)},
        {"text": "Content Intelligence", "grid": (31, 0, 24, 1)},
    ]


def populate_dashboard(dashboard_id: int, dashcards: list[dict]) -> None:
    api_request("PUT", f"/api/dashboard/{dashboard_id}", json={"dashcards": dashcards})
    logger.info("Dashboard layout set with {} dashcards", len(dashcards))


def main():
    authenticate()
    database_id = find_database_id(DATABASE_NAME)

    # Remove stale cards from previous runs
    for stale_name in CARDS_TO_DELETE:
        delete_card_by_name(stale_name)

    # Create all cards
    card_defs = define_cards()
    dashcards = []

    for card_def in card_defs:
        card_id = create_card(
            name=card_def["name"],
            sql=card_def["sql"],
            display=card_def["display"],
            database_id=database_id,
            viz_settings=card_def.get("viz_settings"),
        )
        row, col, size_x, size_y = card_def["grid"]
        dashcards.append(build_dashcard(card_id, row, col, size_x, size_y))

    # Add section headings
    for heading in define_headings():
        row, col, size_x, size_y = heading["grid"]
        dashcards.append(build_heading_dashcard(heading["text"], row, col, size_x, size_y))

    # Create dashboard and populate
    dashboard_id = create_dashboard(
        name=DASHBOARD_NAME,
        description="Analytics dashboard for the HackerNews RSS ingestion pipeline",
    )
    populate_dashboard(dashboard_id, dashcards)

    logger.success("Dashboard ready at {}/dashboard/{}", METABASE_URL, dashboard_id)


if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as e:
        logger.error("HTTP {}: {}", e.response.status_code, e.response.text)
        sys.exit(1)
