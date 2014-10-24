import redis

def runWorker(argv, runFunc):
    _, redis_host, redis_port = argv
    r = redis.Redis(host=redis_host, port=redis_port)
    runFunc(r)