import falcon
import requests
import json
import os
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


class TwitterInputData:

    def __init__(self):
        self.es = Elasticsearch()

        if not self.es.indices.exists(index="userstest"):
            res = self.es.indices.create(index="userstest", body={"mappings": {"numeric_detection": True}})
            #print('users-test created   ',res)

    @staticmethod
    def headers():
        """Authentication info for Twitter Api"""
        bearer_token = os.environ.get('BEARER_TOKEN')
        headers = {'Authorization': 'Bearer {}'.format(bearer_token)}
        return headers

    def get_user_details(self, req, resp):
        """Validates and fetches user data from twitter"""
        username = req.get_param('username') or ''
        userdetails = None
        if not username:
            resp.text = 'No username has been provided'
            resp.status = falcon.HTTP_400
            return

        headers = self.headers()
        url = 'https://api.twitter.com/2/users/by/username/{}'.format(username)
        response = requests.request('GET', url, headers=headers)
        if response.status_code != 200:
            #print(response.status_code, response.text)
            resp.text = response.text
            resp.status = response.status_code
            return
        # user_data = json.dumps(response)
        resp_dict = json.loads(response.text)
        if 'errors' in resp_dict:
            resp.text = response.text
            resp.status = falcon.HTTP_400
            return
        userdetails = resp_dict['data']
        return userdetails

    def get_user_tweets(self, req, resp, userdetails, newest_id=None, pagination_token=None):
        """Fetches a block of tweets and updates them in Elasticsearch. Updates the user data alongside it"""
        headers = self.headers()
        headurl = 'https://api.twitter.com/2/users/{}/tweets?'.format(userdetails['id'])
        urlparams = 'max_results=100&tweet.fields=author_id,created_at&exclude=retweets,replies'
        url = headurl + urlparams
        if newest_id:
            url += '&since_id={}'.format(newest_id)
        if pagination_token:
            url += '&pagination_token={}'.format(pagination_token)
        response = requests.request('GET', url, headers=headers)
        resp_dict = json.loads(response.text)
        meta = resp_dict['meta']
        if not pagination_token:
            self.update_userdetails_elastic(userdetails, meta)

        if 'data' in resp_dict:
            self.update_usertweets_elastic(userdetails, resp_dict['data'])
        if 'next_token' in meta:
            pagination_token = meta['next_token']
        else:
            resp.text = 'The tweets have been updated in Elasticsearch'
            resp.status = falcon.HTTP_200
            return

        return self.get_user_tweets(req, resp, userdetails, None, pagination_token)

    def update_userdetails_elastic(self, userdetails, meta):
        """Updates user details in Elasticsearch"""
        if 'newest_id' in meta:
            newest_id = meta['newest_id']
        else:
            newest_id = '0'
        userdetails['newest_id'] = newest_id
        res = self.es.index(index='userstest', body=userdetails)
        #print(res)


    def get_newest_id(self, userid):
        """Fetches the latest tweet stored in Elasticsearch w.r.t user"""
        query = {'query': {'match': {'id': userid}}}
        userdata = self.es.search(index='userstest', _source=False, docvalue_fields=['id','newest_id'], body=query)
        #print(userdata)
        newest_id = None
        if userdata['hits']['total']['value'] != 0:
            newest_id = userdata['hits']['hits'][0]['fields']['newest_id'][0]
        if newest_id == 0:
            newest_id = None
        return newest_id

    def update_usertweets_and_generate(self, userdetails, data):
        """Updates the tweet details and returns the generator for bulk update"""
        for i in range(len(data)):
            data[i]['author_name'] = userdetails['name']
            data[i]['author_username'] = userdetails['username']
            yield data[i]

    def update_usertweets_elastic(self, userdetails, data):
        """Updates user tweets in Elasticsearch"""
        response = bulk(self.es, self.update_usertweets_and_generate(userdetails, data), index='tweetstest')
        #print(response)




    def on_put(self, req, resp):
        """ Put call for the Update Api"""
        userdetails = self.get_user_details(req, resp)
        #print(userdetails)
        if not userdetails:
            return

        newest_id = self.get_newest_id(userdetails['id'])
        self.get_user_tweets(req, resp, userdetails, newest_id)

