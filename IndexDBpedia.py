"""
Original Source and credit: https://gist.github.com/markharwood/21c723039425b4b3e4277b2bffa5c54c
"""

import bz2, os, re
from GremlinClient import GremlinClient
from urllib.request import unquote
#from elasticsearch import helpers
#from elasticsearch import Elasticsearch
from importlib import reload
import sys

reload(sys)
#sys.setdefaultencoding('utf8')

# Download of data http://downloads.dbpedia.org/2016-04/core-i18n/en/page_links_en.ttl.bz2
filename = 'C:/Users/hug_h/Downloads/page_links_en.ttl.bz2'
indexName = "dbpedialinks"
docTypeName = "article"

linePattern = re.compile(r'<http://dbpedia.org/resource/([^>]*)> <[^>]*> <http://dbpedia.org/resource/([^>]*)>.*',
                         re.MULTILINE | re.DOTALL)

#es = Elasticsearch()

g = GremlinClient()

print("Wiping any existing index...")
#es.indices.delete(index=indexName, ignore=[400, 404])
indexSettings = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
    },
    "mappings": {
        docTypeName: {
            "properties": {
                "subject": {
                    "type": "keyword",
                    "fields": {
                        "text": {
                            "type": "text"
                        }
                    }
                },
                "linked_categories": {
                    "type": "keyword",
                    "fields": {
                        "text": {
                            "type": "text"
                        }
                    }
                },
                "linked_subjects": {
                    "type": "keyword",
                    "fields": {
                        "text": {
                            "type": "text"
                        }
                    }
                }
            }
        }
    }
}
#es.indices.create(index=indexName, body=indexSettings)

actions = []
rowNum = 0
lastSubject = ""
article = {}
numLinks = 0
numOrigLinks = 0


def addLink(article, subject):
    #print("Article Link", article)
    #print("subject Link", subject)
    # Use a separate field for category topics
    if subject.startswith("Category:"):
        article["linked_categories"].append(subject[len("Category:"):])
    else:
        article["linked_subjects"].append(subject)

def newArticle(subject):
    #print("Subject New Article", subject)
    article = {}
    article["subject"] = subject
    article["linked_subjects"] = []
    article["linked_categories"] = []
    addLink(article, subject)
    return article

#'bzip2 -cd ' +

with bz2.open(filename, "rt", encoding="utf-8") as file:
    try:
        for line in file:
            m = linePattern.match(str(line))
            #print(line)
            if m:
                # Lines consist of [from_article_name] [to_article_name]
                # and are sorted by from_article_name so all related items
                # to from_article_name appear in contiguous lines.
                subject = unquote(m.group(1)).replace('_', ' ')
                #print("subject", subject)
                linkedSubject = unquote(m.group(2)).replace('_', ' ')
                #print("linked", linkedSubject)

                if rowNum == 0:
                    article = newArticle(subject)
                    lastSubject = subject
                    numLinks = 0
                    numOrigLinks = 0
                if subject != lastSubject:
                    if len(article["linked_subjects"]) > 1:
                        article["numOrigLinks"] = numOrigLinks
                        article["numLinks"] = numLinks
                        action = {
                            "_index": indexName,
                            '_op_type': 'index',
                            "_type": docTypeName,
                            "_source": article
                        }
                        print("ARTICLE", article)
                        length = len(article["linked_subjects"])
                        vertex = {"id": article["subject"], "no_links": length}
                        g.add_vertex(article["subject"])
                        links = article["linked_subjects"]

                        actions.append(action)
                        # Flush bulk indexing action if necessary
                        if len(actions) >= 5000:
                            #helpers.bulk(es, actions)
                            ## TO check for failures and take appropriate action
                            del actions[0:len(actions)]
                    # Set up a new doc
                    article = newArticle(subject)
                    lastSubject = subject
                    numLinks = 0
                    numOrigLinks = 0

                # Don't want too many outbound links in a single article - slows down things like
                # signif terms and links become tenuous so truncate to max 500
                if len(article["linked_subjects"]) < 500:
                    addLink(article, linkedSubject)
                    numLinks += 1
                numOrigLinks += 1

                rowNum += 1

    except Exception as e:

        print(e)

            #if rowNum % 100000 == 0:
                #print(rowNum, subject, linkedSubject)
