import sys
import requests
import json
from urllib import urlencode
from reddit_scrapper import SUBREDDITS, SUBREDDITS_GEOCODING_QUEUE



YQL_URL = 'https://query.yahooapis.com/v1/public/yql';




def geocode_subreddit(subreddit):
    loc = subreddit['name']
    response = requests.get(YQL_URL+'?'+urlencode({
                                                'q': 'select * from geo.places where text= "%s"'% loc,
                                                'format': 'json'
                                            }))

    place = response.json()['query']['results']['place'][0];
    center = place['centroid']
    center['latitude'] = float(center['latitude'])
    center['longitude'] = float(center['longitude'])
    return {
        'center': center,
        'boundary': place['boundingBox'],
        'woeid': place['woeid'],
        'area': int(place['areaRank'])
    }  



def run(redis_client):
    while True:
        _, subreddit_v = redis_client.blpop(SUBREDDITS_GEOCODING_QUEUE)
        subreddit = json.loads(subreddit_v)
        try:
            geocode_info = geocode_subreddit(subreddit)
        except Exception:
            continue 
       

        print "geocoded subreddit: /r/%s"% subreddit['rid'] 
        subreddit['location'] = geocode_info
        encoded = json.dumps(subreddit)
        redis_client.rpush(SUBREDDITS, encoded)




























