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
import numpy as np
from pymilvus import (
    connections,
    FieldSchema,
    CollectionSchema,
    DataType,
    Collection,
    utility
)
import signal
import spacy
import spacy_sentence_bert
import sys

from news_ingest import connect_to_es

logger = logging.getLogger(__name__)
coloredlogs.install(level="INFO")

cfg = dotenv_values("../.env")

logging.info("Loaded config " + str(dict(cfg)))

DIM = 768

def create_milvus_collection(name: str, dims: int):
    logging.info(f'Building collection {name}')
    fields = [
        FieldSchema(name='_id', dtype=DataType.INT64, is_primary=True, auto_id=False),
        FieldSchema(name='embedding', dtype=DataType.FLOAT_VECTOR, dim=DIM)
    ]

    schema = CollectionSchema(fields, 'title embeddings mapped to doc IDs')
    collection = Collection(name, schema)

    # pulling vectors should be 
    index = { "index_type": "FLAT", "metric_type": "L2", "params": { } }
    collection.create_index(name, index)


def connect_to_milvus(host: str, port: str, check_collection: bool = False):
    def disconnect(sig, frame):
        logging.info("Killing milvus connection")
        connections.disconnect("default")
        sys.exit(0)

    logging.info("Connecting to Milvus vector DB")
    connection = connections.connect(alias="default", host=host, port=port)

    if check_collection and not utility.has_collection('title_embeddings'):
        create_milvus_collection('title_embeddings', DIM)

    # kill the milvus connection on exit
    signal.signal(signal.SIGINT, disconnect)
    return connection


def get_vectors_today(es, milvus, publish_date: str = None):
    if publish_date is None:
        publish_date = date.today().strftime("%Y-%m-%d")

    all_docs = es.search(
        index="raw_articles",
        body={
            "query": {
                "range": {
                    "published_at": {
                        "gte": "now-10d/d",
                        "lte": "now",
                    }
                }
            },
            "sort": {"popularity": {"order": "desc"}},
            "size": 50,
        },
    )
    all_titles = list(map(lambda d: d["_source"]["title"], all_docs["hits"]["hits"]))

    if all_titles == []:
        logging.warning("Titles list is empty, your query did not yield any titles")
    logging.debug(all_titles)

    # generate all title vectors for these documents
    nlp = spacy_sentence_bert.load_model("en_stsb_distilbert_base")
    
    vectors = []
    for i, doc in enumerate(nlp.pipe(all_titles, n_process=4)):
        vectors += [np.array(doc.vector, dtype=np.float32)]
        # TODO - extract NER and add to ES

    vector_ids = [
        # convert to a 64 bit int for storage in the vector DB
        int.from_bytes(doc["_id"][:8].encode("utf8"), "little")
        for doc in all_docs["hits"]["hits"]
    ]

    logging.info('Inserting vector data')
    Collection('title_embeddings').insert(
        [vector_ids, vectors]
    )
    logging.info('Done')

if __name__ == "__main__":
    milvus_host = cfg["MILVUS_HOST"]
    milvus_port = cfg["MILVUS_PORT"]

    milvus = connect_to_milvus(milvus_host, milvus_port, True)
    es = connect_to_es()

    get_vectors_today(es, milvus)
