## Local Subreddit Scraper

### About this scraper
We wrote this tiny hack for NewsMapper to fetch topics from local subreddits. 
Depending on network condition, it takes around 2 minutes for the scraper to 
fetch the top 25 topics from all geocodable local subreddits (currently there
are about 2000 such subreddits).

### Prerequisite
* redis
* other python dependency... you can get them with this command
```
$ pip install -r requirements.txt
```

### Running the scraper
1. Modify `config.py` for you redis server.
2. Run the scraper with command below:
```
$ python run.py
```

