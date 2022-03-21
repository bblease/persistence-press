#!usr/bin/env python3

"""
📰 Article ingest into ElasticSearch

Creates needed index if ES has not been first initialized
"""

import coloredlogs, logging
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
    es = Elasticsearch(
        [{"scheme": "http", "host": cfg["ES_HOST"], "port": int(cfg["ES_PORT"])}]
    )

    # first time document loading
    if check_indices and not es.indices.exists(index="raw_articles"):
        es.indices.create("raw_articles")

    return es


def ingest_articles(es) -> List[Dict]:
    url = cfg["MEDIASTACK_URL"]
    api_key = cfg["MEDIASTACK_TOKEN"]

    try:
        params = {
            "access_key": api_key,
            "sources": ",".join(["us", "en"]),
            "sort": "popularity",
            "limit": 100,
        }
        result = requests.get(url, params=params)

        if "error" in result:
            raise ValueError("Returned response has encountered an error")
        else:
            logger.info("Success")
            logger.info("Loading to elasticsearch")
            
            all_articles = result.json()['data']
            article_lst = [
                {
                    "_index": "raw_articles",
                    "_op_type": "index",
                    "_id": str(hashlib.md5(article_data['title'].encode('utf-8')).hexdigest()),
                    "_source": article_data,
                }
                for article_data in all_articles
            ]
            helpers.bulk(es, article_lst)

            return all_articles

    except ValueError as e:
        logging.error(e)
        raise(e)


if __name__ == "__main__":
    es = connect_to_es()
    ingest_articles(es)
