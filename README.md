# Scraping Pipeline Project

## Overview
This project scrapes JSON data, stores it in PostgreSQL, and caches it in Redis

## Setup
1. Ensure Docker and Docker Compose are installed.
2. Clone this repository.
3. Run the following commands to start the services:
   ```bash
   make build
   
6. run scrapy in container
   ```bash
   make scrapy
   
5. for exporting to CSV:
   ```bash
   make export


