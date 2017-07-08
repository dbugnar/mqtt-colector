from pymongo import MongoClient
import json
import os

#client = MongoClient("mongodb://colector:pppp@ds013340.mlab.com:13340/mqtt-database")

data_base = os.getenv('DB')
if data_base is None:
    print('Data base not found as a environment variable')
    exit(1)
client = MongoClient(data_base)

db = client['mqtt-database']


def db_get_topics():
    topics = []
    collection=db['topics']
    cursor = collection.find({})
    for document in cursor:
        for topic in document['topics']:
            topics.append(topic)
    return topics


def db_add_topic(topic):
    collection = db['topics']
    document = collection.find_one()
    id_var = {}
    id_var['_id'] = document['_id']
    topics = document['topics']
    topics.append(topic)
    collection.update(id_var , {'topics' : topics})


def db_update_broker(key, value):
    #print(key, value)
    collection = db['broker']
    document = collection.find_one()
    id_var = {}
    id_var['_id'] = document['_id']
    collection.update(id_var, {'$set': {key: value}}, upsert=True)


def db_add_message(topic, message_obj):
    #print(key, value)
    collection = db['messages']
    document = collection.find_one()
    id_var = {}
    id_var['_id'] = document['_id']
    try:
        messages = document[topic]
    except KeyError:
        messages = []
    messages.append(json.loads(message_obj.to_json()))
    collection.update(id_var, {'$set': {topic: messages }}, upsert=True)


# if __name__ == '__main__':
#      get_topics()
#      add_topic('aa')