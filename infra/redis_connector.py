import redis

def get_redis_connection():
    return redis.Redis(host='redis_service', port=6379, db=0)