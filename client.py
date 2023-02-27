from datetime import datetime
from elasticsearch import Elasticsearch

# connection to elastic
es = Elasticsearch(
            ["https://elastic:sTUr5OyBnO-+T=kopJoL@localhost:9200"],
            verify_certs=True,
            use_ssl=True,
            ca_certs='../elasticsearch_file/config/certs/http_ca.crt',
        )
# e=es.options()
# verify connection
# print(es.ping())

def Createindex():
    es.indices.create(index='test-index', ignore=[400,401])

def get_all_indices():
    indices=es.indices.get_alias("*")
    print(indices)

def delete_index():
    es.indices.delete(index='test-index-23_02_2023', ignore=[400, 404])

def searchforindex():
    try:
        resp = es.search(index="products")
        print(resp)
    except Exception as e:
        print("Index does not exist")
        print(e)

def search_if_index_exits():
    index = "reviews*"
    try:
        resp = es.search(index="reviews*")
        print(resp["_shards"]["total"])
    except Exception as e:
        print("Index does not exist")

def inserting_doc():
    doc = {
    'author': 'author_name',
    'text': 'Interensting content...',
    'timestamp': datetime.now(),
    }
    resp = es.index(index="test-index", id=1, document=doc)
    print(resp)

def getting_doc():
    resp = es.get(index="test-index", id=1)
    print(resp['_source'])

def refreshing_doc():
    es.indices.refresh(index="test-index")

def searching_doc():
    resp = es.search(index="test-index", query={"match_all": {}})
    print(resp)
    # print("Got %d Hits:" % resp['hits']['total']['value'])
    # for hit in resp['hits']['hits']:
    #     print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])
def updating_doc():
    doc = {
    'author': 'author_name',
    'text': 'Interensting modified content...',
    'timestamp': datetime.now(),
    }
    resp = es.update(index="test-index", id=1, doc=doc)
    print(resp['result'])

updating_doc()