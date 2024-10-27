import os
from twisted.internet import reactor

BOT_NAME = 'jobs_project'
SPIDER_MODULES = ['jobs_project.spiders']
NEWSPIDER_MODULE = 'jobs_project.spiders'

ITEM_PIPELINES = {
    'jobs_project.pipelines.RedisCachePipeline': 300,
    'jobs_project.pipelines.PostgreSQLPipeline': 400,
}

REQUEST_FINGERPRINTER_IMPLEMENTATION = '2.7'

POSTGRES_SETTINGS = {
    'dbname': 'database_name',
    'user': 'user',
    'password': 'password',
    'host': 'postgres_service',
    'port': '5432'
}
