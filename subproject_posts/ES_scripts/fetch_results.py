import pprint as pp
from elasticsearch import Elasticsearch
import optparse

parser = optparse.OptionParser()
parser.add_option("--DOMAIN",dest = "DOMAIN",help = "enter DOMAIN")
(options,arguments) = parser.parse_args()

DOMAIN = options.DOMAIN
#DOMAIN  = "www.kernelmode.info"

# Do Not Modify
INDEX_FORUM = "forum_posts"
INDEX_AUTHOR = "forum_author"


def fetch_elastic_results():
    es = Elasticsearch(['10.2.0.90:9342'])
    #query = {'query_string': {'use_dis_max': 'true', 'query': {'domain:{0}'.format(DOMAIN)}}  
    query = {'query_string': {'use_dis_max': 'true', 'query': 'domain: {0}'.format(DOMAIN)}}
    
    print("Printing posts results")
    forum_results = es.search(index=INDEX_FORUM, body={"query": query})
    pp.pprint(forum_results)
    
    '''for repeat_count in range(1,4):
        print('*'*160)
    
    print("Printing author results")
    author_results = es.search(index=INDEX_AUTHOR, body={"query": query})
    pp.pprint(author_results)'''

if __name__ == "__main__":
    fetch_elastic_results()
