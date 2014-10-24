import subprocess
import os
from time import time
import redis
import requests
import simplejson as json
from lxml import etree
from reddit_scrapper import TEMP_SUBREDDITS, SUBREDDITS_GEOCODING_QUEUE
from config import *



dirname = os.path.dirname(os.path.abspath(__file__))

SCRAPPER_LOC = os.path.join(dirname, 'reddit_scrapper.py')
GEOCODER_LOC = os.path.join(dirname, 'subreddit_geocoder.py')

REGIONS = [
    'europe',
    'asia',
    'oceania',
    'africa',
    'northamerica',
    'southamerica',
]

REDDIT_WIKI = 'http://www.reddit.com/r/LocationReddits/wiki/faq/'
SCRAPING_WORKER_COUNT = 20 
GEOCODING_WORKER_COUNT = 30
WORKERS = []
TIMEOUT = 20 



def find_subreddits(redis_client, region):
    wiki_page = requests.get(REDDIT_WIKI+region, headers=AGENT_HEADER)
    if wiki_page.status_code != 200:
        print wiki_page.status_code
        return 0

    wiki = etree.HTML(wiki_page.text)

    count = 0

    for subreddit in wiki.xpath("//a[starts-with(@href, '/r/') and contains(@rel, 'nofollow')]"):
        name = subreddit.text.strip().lower()
        if name == 'index':
            continue
        r_info = {
            'name': name,
            'rid': subreddit.attrib['href'].split('/')[-1]
        }
        redis_client.rpush(TEMP_SUBREDDITS, json.dumps(r_info))
        print 'found location with subreddit: '+name
        count += 1

    return count




def spawn_scraping_worker(redis_host, redis_port):
    return subprocess.Popen('python %s %s %s' % 
                                (SCRAPPER_LOC,
                                 redis_host,
                                 redis_port), 
                                shell=True)

        


def spawn_geocoding_worker(redis_host, redis_port):
    return subprocess.Popen('python %s %s %s' % 
                                    (GEOCODER_LOC,
                                    redis_host,
                                    redis_port), 
                                     shell=True)





if __name__ == '__main__':
    begin = time()

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    r.delete(TEMP_SUBREDDITS)
    r.delete(SUBREDDITS)
    r.delete(SUBREDDITS_GEOCODING_QUEUE)

    workers = []

    for i in xrange(SCRAPING_WORKER_COUNT):
        workers.append(spawn_scraping_worker(REDIS_HOST, REDIS_PORT))
        print 'spawning scraping worker %d' % i

    for i in xrange(GEOCODING_WORKER_COUNT):        
        workers.append(spawn_geocoding_worker(REDIS_HOST, REDIS_PORT))
        print 'spawning geocoding worker %d' % i


    for region in REGIONS:
        find_subreddits(r, region)


    monitor = r.brpoplpush(SUBREDDITS_GEOCODING_QUEUE, SUBREDDITS_GEOCODING_QUEUE, TIMEOUT)
    while monitor is not None:
        monitor = r.brpoplpush(SUBREDDITS_GEOCODING_QUEUE, SUBREDDITS_GEOCODING_QUEUE, TIMEOUT)
    

    for worker in workers:
        worker.kill()

    count = len(r.lrange(SUBREDDITS, 0, -1)) 
 

    end = time()

    print 'done.'
    print '%d subreddits scraped.' % count
    print 'took %.2f seconds' % (end - begin)

    
    



























