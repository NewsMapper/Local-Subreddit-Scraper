import sys
from lxml import etree
import requests
import redis
import simplejson as json
from config import *



SUBREDDITS_GEOCODING_QUEUE = 'GEOCODING:SUBREDDITS:QUEUE'
TEMP_SUBREDDITS = '%s:%s' % (SUBREDDITS, 'TEMP')




def get_subbreddit_key(rid):
    return '%s:%s' % (SUBREDDIT_KEY, rid.lower())



def construct_item(title, vote, image):
    if image is None:
        image_src = None
    else:
         image_src = image.attrib['src']

    if title['link'].startswith('/r/'):
        title['link'] = 'http://www.reddit.com'+title['link']

    return {
        'title': title,
        'vote': vote,
        'image': image_src,
    }



def parse_subreddit(page):
    html = etree.HTML(page)
    titles = [{
            'text': title.text,
            'link': title.attrib['href']
        } for title in html.xpath(".//a[contains(@class, 'title')]")]
    votes = [vote.text for vote in html.xpath(".//div[@class='score unvoted']")]
    images = [img.find('img') for img in html.xpath(".//a[contains(@class, 'thumbnail')]")]

    return map(construct_item, titles, votes, images)




def scrape_subreddit(redis_client, subreddit):
    rid = json.loads(subreddit)['rid']
    page = requests.get('http://www.reddit.com/r/'+rid, headers=AGENT_HEADER, allow_redirects=False)
    if page.status_code == 200:
        reddits = parse_subreddit(page.text)
        redis_client.set(get_subbreddit_key(rid), json.dumps(reddits))
        # wait for geocoding worker to geocode stuff
        redis_client.rpush(SUBREDDITS_GEOCODING_QUEUE, subreddit)
        print 'fetched /r/'+rid
    else:
        print page.status_code, rid




def run(redis_client):
    while True:
        _, rid = redis_client.blpop(TEMP_SUBREDDITS)
        scrape_subreddit(redis_client, rid)
















































