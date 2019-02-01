import pprint
from elasticsearch import Elasticsearch

import fetch_configuration


pp = pprint.PrettyPrinter(indent=fetch_configuration.INDENT_SPACES)

def fetch_elastic_results():
    es = Elasticsearch(['10.2.0.90:9342'])
    query = {'query_string': {'use_dis_max': 'true', 'query': 'domain: {0}'.format(fetch_configuration.DOMAIN)}}

    #print("Printing posts results")
    forum_results = es.search(index=fetch_configuration.INDEX_FORUM, body={"query": query})
    pp.pprint(forum_results)
    
    for repeat_count in range(1,4):
        print('*'*fetch_configuration.REPEAT_COUNT)
    
    print("Printing author results")
    author_results = es.search(index=fetch_configuration.INDEX_AUTHOR, body={"query": query})
    pp.pprint(author_results)

if __name__ == "__main__":
    fetch_elastic_results()
