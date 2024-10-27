
# Build and start Docker containers
build:
	docker-compose up --build

# Run Scrapy spider in container
scrapy:
	docker-compose run scrapy crawl job_spider

# Export to CSV by running query.py
export:
	docker-compose run scrapy python3 query.py