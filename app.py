import falcon
from waitress import serve
from twitter_data_input import TwitterInputData
from fetch_tweets import FetchTweets

app = application = falcon.App()

dataInput = TwitterInputData()
app.add_route('/update', dataInput)
fetchTweets = FetchTweets()
app.add_route('/fetch', fetchTweets)


if __name__ == '__main__':
    serve(app)
