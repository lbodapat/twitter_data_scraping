import os
import pysolr
import requests

# https://tecadmin.net/install-apache-solr-on-ubuntu/


CORE_NAME = "IRF21P4"
AWS_IP = "18.219.230.238"


# [CAUTION] :: Run this script once, i.e. during core creation

def delete_core(core=CORE_NAME):
    print(os.system('sudo su - solr -c "/opt/solr/bin/solr delete -c {core}"'.format(core=core)))


def create_core(core=CORE_NAME):
    print(os.system(
        'sudo su - solr -c "/opt/solr/bin/solr create -c {core} -n data_driven_schema_configs"'.format(
            core=core)))

class Indexer:
    def __init__(self):
        self.solr_url = f'http://{AWS_IP}:8983/solr/'
        self.connection = pysolr.Solr(self.solr_url + CORE_NAME, always_commit=True, timeout=5000000)
    def do_initial_setup(self):
       delete_core()
       create_core()

    def create_documents(self, docs):
        print(self.connection.add(docs))

    def add_fields(self):
        '''
        Define all the fields that are to be indexed in the core. Refer to the project doc for more details
        :return:
        '''
        data = {
            "add-field": [
                {
                    "name": "poi_name",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "poi_id",
                    "type": "plong",
                    "multiValued": False
                }, {
                    "name": "verified",
                    "type": "boolean",
                    "multiValued": False
                },
                {
                    "name": "country",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "replied_to_tweet_id",
                    "type": "plong",
                    "multiValued": False
                },
                {
                    "name": "replied_to_user_id",
                    "type": "plong",
                    "multiValued": False
                },
                {
                    "name": "reply_text",
                    "type": "text_general",
                    "multiValued": False,
                    "indexed": True
                },
                {
                    "name": "tweet_text",
                    "type": "text_general",
                    "multiValued": False,
                    "indexed": True
                },
                {
                    "name": "tweet_lang",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "text_en",
                    "type": "text_en",
                    "multiValued": False,
		    "indexed": True
                },
                {
                    "name": "text_hi",
                    "type": "text_hi",
                    "multiValued": False,
		    "indexed": True
                },
                {
                    "name": "text_es",
                    "type": "text_es",
                    "multiValued": False,
		    "indexed": True
                },
                {
                    "name": "hashtags",
                    "type": "strings",
                    "multiValued": True
                },
                {
                    "name": "mentions",
                    "type": "strings",
                    "multiValued": True
                },
                {
                    "name": "tweet_urls",
                    "type": "strings",
                    "multiValued": True
                },
                {
                    "name": "tweet_emoticons",
                    "type": "strings",
                    "multiValued": True
                },
                {
                    "name": "tweet_date",
                    "type": "pdate",
                    "multiValued": False
                },
                {
                    "name": "geolocation",
                    "type": "strings",
                    "multiValued": True
                },
                {
                    "name": "followers_count",
                    "type": "plong",
                    "multiValued": False
                },
                {
                    "name": "retweet_count",
                    "type": "plong",
                    "multiValued": False
                },
                {
                    "name": "profile_image_url_https",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "favorite_count",
                    "type": "plong",
                    "multiValued": False
                },
                {
                    "name": "media_url",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "screen_name",
                    "type": "string",
                    "multiValued": False
                }
            ]
        }
        
        print(requests.post(self.solr_url + CORE_NAME + "/schema", json=data).json())


if __name__ == "__main__":
    i = Indexer()
    i.do_initial_setup()
    i.add_fields()
