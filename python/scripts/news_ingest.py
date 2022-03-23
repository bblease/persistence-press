#!usr/bin/env python3

"""
📰 Article ingest into ElasticSearch

Creates needed index if ES has not been first initialized
"""

import coloredlogs, logging
from datetime import date
from elasticsearch import helpers
from elasticsearch.client import Elasticsearch
import hashlib
import json
import requests
from typing import List, Dict

from dotenv import dotenv_values

logger = logging.getLogger(__name__)
coloredlogs.install(level="DEBUG")

cfg = dotenv_values("../.env")

logging.info("Loaded config " + str(dict(cfg)))


def connect_to_es(check_indices: bool = False):
    """
    Connect to ElasticSearch

    Parameters:
        check_indicies - check if the indexes exist, create if not

    Returns:
        the connected ES instance
    """
    es = Elasticsearch(
        [{"scheme": "http", "host": cfg["ES_HOST"], "port": int(cfg["ES_PORT"])}]
    )

    # first time document loading
    if check_indices and not es.indices.exists(index="raw_articles"):
        es.indices.create("raw_articles")

    return es


def ingest_articles(
    es, news_api_url, news_api_key, offset: int = 0, remaining: int = None
) -> List[Dict]:
    """
    Pull articles from Mediastack (currently) and upload to ES

    Parameters:
        news_api_url - the url for the News api (e.g. NewsAPI or MediaStack)
        news_api_key - the API key needed for API authentication
        offset - the pagination offset for larger article sets
        remaining - the number of articles remaining in the set

    Notes:
        remaining is initially None and populated with the pagination results
        from Mediastack

    """
    try:
        params = {
            "access_key": news_api_key,
            "languages": "en",
            "countries": "us",
            "sort": "popularity",
            "limit": 100,
            "offset": offset,
            "date": date.today().strftime("%Y-%m-%d"),
        }
        result = requests.get(news_api_url, params=params)
        result = result.json()

        if "error" in result:
            logging.error(result)
            raise ValueError("Returned response has encountered an error")
        else:
            logger.info("Success")

            # calculate further values for fetching more data
            count = len(result["data"])
            total = result["pagination"]["total"]
            if remaining is None:
                remaining = total - count
            else:
                remaining -= count

            logger.info(f"{count} articles returned")
            logging.info(f"{remaining} articles remaining")
            logger.info("Loading to elasticsearch")

            all_articles = result["data"]
            article_lst = [
                {
                    "_index": "raw_articles",
                    "_op_type": "index",
                    "_id": str(
                        hashlib.md5(article_data["title"].encode("utf-8")).hexdigest()
                    ),
                    "_source": {**article_data, "popularity": (i + offset) / total},
                }
                for i, article_data in enumerate(all_articles)
            ]
            helpers.bulk(es, article_lst)
            logger.info("Done")

    except ValueError as e:
        logging.error(e)
        raise (e)

    if remaining == 0:
        return
    else:
        ingest_articles(es, news_api_url, news_api_key, offset + 100, remaining)


if __name__ == "__main__":
    es = connect_to_es()

    url = cfg["MEDIASTACK_URL"]
    api_key = cfg["MEDIASTACK_TOKEN"]
    ingest_articles(es, url, api_key)
