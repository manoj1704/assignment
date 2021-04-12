import json
import falcon
from elasticsearch import Elasticsearch


class FetchTweets:

    def __init__(self):
        self.es = Elasticsearch()

    def on_get(self, req, resp):
        """Get call for the Fetch Api"""
        page = req.get_param_as_int('page', default=1)
        limit = req.get_param_as_int('limit', default=10)
        username = req.get_param('username')
        name = req.get_param('name')
        text = req.get_param('text')
        start = max((page - 1) * limit, 0)

        if limit > 10000:
            resp.text = 'Page Size exceeded'
            resp.status = falcon.HTTP_400
            return
        if page * limit > 10000:
            resp.text = 'Request window too large'
            resp.status = falcon.HTTP_400
            return

        sort = [{'created_at': {'order': 'desc'}}]
        match_list = []
        if username:
            d = {'match': {'author_username': username}}
            match_list.append(d)
        if name:
            d = {'match': {'author_name': name}}
            match_list.append(d)
        if text:
            d = {'match': {'text': text}}
            match_list.append(d)

        body = {'sort': sort}
        if len(match_list) == 1:
            body['query'] = match_list[0]
        else:
            body['query'] = {'bool': {'must': match_list}}

        response = self.es.search(index='tweetstest', body=body, from_=start,
                                  size=limit)
        #, filter_path=['hits.hits._source*']
        # using=self.es, index='tweets-test').query('match', username='gvanrossum').sort('-created_at')
        #print(response)

        result = []
        for hit in response['hits']['hits']:
            result.append(hit['_source'])
        resp.body = json.dumps(result)
        resp.status = falcon.HTTP_200
