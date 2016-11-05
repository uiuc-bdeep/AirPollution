from __future__ import absolute_import, print_function
from __future__ import unicode_literals

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
from pymongo import MongoClient


client = MongoClient('141.142.209.153')
db = client['twitter']


consumer_key= 'Wh2eNViH7cfifDeKU27lY51AA'
consumer_secret= '9ODXPNo3N3uHgDZ1IUuOSSvTyxOB9yhCrPbE1xYFTTUoDFXvlI'
access_token= '1687248824-Q4WgBScD4cu6Jr4LIjHCwK3xDDHUx7HBTPIXeiv'
access_token_secret= 'x5qqJhFDXaQaBE95E3YuJtggPF2TcRFvqrHTghtXoByHa'
#override tweepy.StreamListener to add logic to on_status
class StdOutListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    def on_data(self, data):
        #print(type(data.encode('utf-8')))
        a = data.encode('utf-8')
        d = json.loads(a)
        try:
        	lon = d['coordinates']['coordinates'][0]
        	lat = d['coordinates']['coordinates'][1]
        	text = d['text']
        	#print('here')
        	creat_time = d['created_at']
        	tw = {
        		"lat":lat,
        		"lon":lon,
        		"text":text,
        		"creat_time":creat_time
        	}
        	print ("here")
        	db.geotweet.insert(tw)
        except:
        	print('no coordinates')

        return True

    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)
    stream.filter(locations=[-91.202345,37.182334,-87.069500,42.475548])

