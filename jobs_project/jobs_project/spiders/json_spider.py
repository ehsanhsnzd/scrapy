import json
import scrapy
import os
from jobs_project.jobs_project.items import JobItem

class JobSpider(scrapy.Spider):
    name = 'job_spider'
    custom_settings = {
        'ITEM_PIPELINES': {
            'jobs_project.pipelines.RedisCachePipeline': 300,
            'jobs_project.pipelines.PostgreSQLPipeline': 400,
        },
    }

    def start_requests(self):
        json_file_path = '/app/data/s01.json'

        with open(json_file_path, 'r') as f:
                data = json.load(f)
                jobs = data.get("jobs")

                for job_entry in jobs:
                    job_data = job_entry.get("data") if isinstance(job_entry, dict) else None

                    apply_url = job_data.get("apply_url", "http://url.com")  # Fallback if no URL is found
                    yield scrapy.Request(
                        url=apply_url,
                        callback=self.parse_page,
                        meta={'job_data': job_data}
                    )

    def parse_page(self, response):
        job_data = response.meta.get('job_data', {})
        try:
            item = JobItem(
                slug=job_data.get("slug"),
                language=job_data.get("language"),
                req_id=job_data.get("req_id"),
                title=job_data.get("title"),
                description=job_data.get("description"),
                street_address=job_data.get("street_address"),
                city=job_data.get("city"),
                state=job_data.get("state"),
                country_code=job_data.get("country_code"),
                postal_code=job_data.get("postal_code"),
                latitude=job_data.get("latitude"),
                longitude=job_data.get("longitude"),
                categories=[cat.get("name") for cat in job_data.get("categories", []) if isinstance(cat, dict)],
                tags=job_data.get("tags"),
                brand=job_data.get("brand"),
                employment_type=job_data.get("employment_type"),
                hiring_organization=job_data.get("hiring_organization"),
                apply_url=job_data.get("apply_url"),
                create_date=job_data.get("create_date"),
                meta_data = job_data.get("meta_data", {}),
                googlejobs = job_data.get("meta_data", {}).get("googlejobs", {})
            )
            yield item

        except Exception as e:
            spider.logger.error(f"Error inserting item: {e}")

