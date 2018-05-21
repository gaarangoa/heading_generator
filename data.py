import unicodedata
import json
from pymongo import MongoClient
import random
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from collections import Counter
from tqdm import tqdm

import spacy
from spacy.lang.es.examples import sentences

nlp = spacy.load('es_core_news_sm')

# start mongo database
# mongod --dbpath /Volumes/drive/projects/umayux/models/mongo/ --port 11914 --directoryperdb

client = MongoClient("mongodb://localhost", 11914)
db = client['twitter']

data = {}
# traverse the data to find tweets that are parents (headers)
for tweet in db['eltiempo'].find()[1:]:
    if tweet['has_parent_tweet'] == False:
        data[ tweet['_id'] ] = tweet

# traverse the data to process childs
for tweet in db['eltiempo'].find()[1:]:
    if tweet['has_parent_tweet'] == "true":
        try:
            data[tweet['parent']]['comments'].append(tweet['text'].replace('\t', ''))
        except Exception as e:
            try:
                data[ tweet['parent'] ]['comments'] = [tweet['text'].replace('\t', '')]
            except:
                pass

data2 = []
for i in data:
    try:
        data2.append( ["\t".join(data[i]['comments']).replace('\n', ''), data[i]['text']] )
    except Exception as e:
        pass


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

data3 = []
for i in tqdm(data2):
    for k in chunks( i[0].split('\t'), 5 ):
        doc = nlp(" ".join(k))
        rdoc = " ".join([token.text for token in doc if token.pos_ in ["NOUN", 'VERB', 'AUX'] ])
        if len(i[1]) > 50 and len( rdoc )>100:
            data3.append(rdoc+"\t"+i[1].replace('\t', '').replace('\n','')+"\n")
        continue

random.shuffle(data3)

to = open('./data/train/data.txt', 'w')
tv = open('./data/dev/data.txt', 'w')
tt = open('./data/test/data.txt', 'w')

print('len data', len(data3))

for ix,item in enumerate(data3):
    if ix < 0.7*len(data3):
        to.write(item.lower())
    elif 0.7*len(data3) <= ix <= 0.9*len(data3):
        tv.write(item.lower())
    elif ix > 0.9*len(data3):
        tt.write(item.lower())
