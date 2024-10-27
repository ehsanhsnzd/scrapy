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