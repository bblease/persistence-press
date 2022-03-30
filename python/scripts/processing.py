#!usr/bin/env python3

"""
🤖 Run named entity recognition and Google trends popularity analysis

Uses multithreading to parallelize the workload
Upload the final results to ES when finished
"""

import coloredlogs, logging
from datetime import date, datetime
from dotenv import dotenv_values
from elasticsearch.client import Elasticsearch
from pymilvus import connections
import signal
import spacy_sentence_bert
import sys

from news_ingest import connect_to_es

logger = logging.getLogger(__name__)
coloredlogs.install(level="DEBUG")

cfg = dotenv_values("../.env")

logging.info("Loaded config " + str(dict(cfg)))

def connect_to_milvus(host: str, port: str):
    def disconnect(sig, frame):
        logging.info('Killing milvus connection')
        connections.disconnect('default')
        sys.exit(0)

    logging.info('Connecting to Milvus vector DB')
    connection = connections.connect(
        alias='default',
        host=host,
        port=port
    )

    # kill the milvus connection on exit
    signal.signal(signal.SIGINT, disconnect)
    return connection


def get_vectors_today(es, publish_date: str = None):
    if publish_date is None:
        publish_date = date.today().strftime("%Y-%m-%d")

    all_docs = es.search(index='raw_articles', body={
        "query": {
            "range": {
                "published_at": {
                    "gte": f"{publish_date}T00:00:00",
                    "lte": f"{publish_date}T23:59:59"
                }
            }
        }

    })

    print(len(all_docs))
    print(all_docs['hits']['hits'])

    # nlp = spacy_sentence_bert.load('stsb-distilbert-base')
    # for doc in nlp.pipe(all_docs, n_process=4):
    #     print(doc_1.vector.shape)




if __name__ == "__main__":
    milvus_host = cfg['MILVUS_HOST']
    milvus_port = cfg['MILVUS_PORT']

    milvus = connect_to_milvus(milvus_host, milvus_port)
    es = connect_to_es()

    get_vectors_today(es)

