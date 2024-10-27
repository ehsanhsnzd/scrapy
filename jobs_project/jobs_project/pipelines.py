from infra.postgresql_connector import get_postgres_connection
from infra.redis_connector import get_redis_connection

class PostgreSQLPipeline:
    def open_spider(self, spider):
        self.conn = get_postgres_connection()
        self.cursor = self.conn.cursor()
        self.cursor.execute("""
            create table if not exists jobs (
                id SERIAL PRIMARY KEY,
                slug VARCHAR(100) UNIQUE,
                language VARCHAR(10),
                req_id VARCHAR(50),
                title VARCHAR(255),
                description TEXT,
                city VARCHAR(100),
                state VARCHAR(100),
                postal_code VARCHAR(20),
                latitude FLOAT,
                longitude FLOAT,
                brand VARCHAR(100),
                employment_type VARCHAR(50),
                hiring_organization VARCHAR(100),
                apply_url TEXT,
                create_date TIMESTAMP
            )
        """)
        self.cursor.execute("""
            create table if not exists categories (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) UNIQUE
            )
        """)
        self.cursor.execute("""
            create table if not exists job_categories (
                job_id INTEGER REFERENCES jobs(id) ON DELETE CASCADE,
                category_id INTEGER REFERENCES categories(id) ON DELETE CASCADE,
                PRIMARY KEY (job_id, category_id)
            )
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS meta_data (
                id SERIAL PRIMARY KEY,
                job_id INT REFERENCES jobs(id) ON DELETE CASCADE,
                ats VARCHAR(255),
                ats_instance VARCHAR(255),
                client_code VARCHAR(255),
                district_description VARCHAR(255),
                domicile_location VARCHAR(255),
                region_description VARCHAR(255),
                canonical_url TEXT,
                last_mod TIMESTAMP
            )
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS googlejobs (
                id SERIAL PRIMARY KEY,
                job_id INT,
                company_name VARCHAR(255),
                job_name VARCHAR(255),
                job_hash VARCHAR(255),
                job_summary TEXT,
                job_title_snippet TEXT,
                search_text_snippet TEXT
            )
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS meta_data_question_sets (
                id SERIAL PRIMARY KEY,
                meta_data_id INT,
                name VARCHAR(255),
                ordinal INT
            )
        """)
        self.conn.commit()

    def close_spider(self, spider):
        self.conn.commit()
        self.cursor.close()
        self.conn.close()

    def process_item(self, item, spider):
        if item is None:
            print('checkredis')
            print(item)
            return

        try:
            self.cursor.execute("""
                INSERT INTO jobs (slug, language, req_id, title, description, city, state, postal_code, latitude, longitude,
                                  brand, employment_type, hiring_organization, apply_url, create_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (slug) DO NOTHING
            """, (
                item['slug'], item['language'], item['req_id'], item['title'], item['description'],
                item['city'], item['state'], item['postal_code'], item['latitude'], item['longitude'],
                item['brand'], item['employment_type'], item['hiring_organization'],
                item['apply_url'], item['create_date']
            ))
            self.conn.commit()

            self.cursor.execute("SELECT id FROM jobs WHERE slug = %s", (item['slug'],))
            job_id = self.cursor.fetchone()[0]
            for category_name in item['categories']:
                self.cursor.execute("""
                    INSERT INTO categories (name) VALUES (%s) ON CONFLICT (name) DO NOTHING
                """, (category_name,))
                self.cursor.execute("SELECT id FROM categories WHERE name = %s", (category_name,))
                category_id = self.cursor.fetchone()[0]
                self.conn.commit()

                self.cursor.execute("""
                    INSERT INTO job_categories (job_id, category_id) VALUES (%s, %s) ON CONFLICT DO NOTHING
                """, (job_id, category_id))
                self.conn.commit()

            meta_data = item.get('meta_data', {})

            if meta_data:
                self.cursor.execute("""
                    INSERT INTO meta_data (job_id, ats, ats_instance, client_code, district_description, domicile_location,
                                           region_description, canonical_url, last_mod)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    job_id, meta_data.get('ats'), meta_data.get('ats_instance'), meta_data.get('client_code'),
                    meta_data.get('district_description'), meta_data.get('domicile_location'),
                    meta_data.get('region_description'), meta_data.get('canonical_url'),
                    meta_data.get('last_mod')
                ))
                self.conn.commit()

                meta_data_id = self.cursor.lastrowid
                for question_set in meta_data.get('question_sets', []):
                    self.cursor.execute("""
                        INSERT INTO meta_data_question_sets (meta_data_id, name, ordinal) VALUES (%s, %s, %s)
                    """, (meta_data_id, question_set.get('name'), question_set.get('ordinal')))
                    self.conn.commit()

            googlejobs = item.get('googlejobs', {})
            if googlejobs:
                self.cursor.execute("""
                    INSERT INTO googlejobs (job_id, company_name, job_name, job_hash, job_summary,
                                            job_title_snippet, search_text_snippet)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    job_id, googlejobs.get('companyName'), googlejobs.get('jobName'), googlejobs.get('jobHash'),
                    googlejobs.get('jobSummary'), googlejobs.get('jobTitleSnippet'), googlejobs.get('searchTextSnippet')
                ))
                self.conn.commit()

        except Exception as e:
            spider.logger.error(e)
            self.conn.rollback()

        return item

class RedisCachePipeline:
    def open_spider(self, spider):
        self.redis = get_redis_connection()

    def process_item(self, item, spider):
        key = f"job:{item['slug']}"
        if self.redis.exists(key):
            return None

        self.redis.set(key, item['slug'])
        return item
