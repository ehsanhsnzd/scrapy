import csv
from infra.postgresql_connector import get_postgres_connection

class DatabaseQuery:
    def __init__(self):
        self.conn = get_postgres_connection()
        self.cursor = self.conn.cursor()

    def fetch_all_jobs(self):
        self.cursor.execute("select * from jobs")
        jobs = self.cursor.fetchall()
        return jobs

    def export_to_csv(self, file_path='/app/output/output.csv'):
        jobs = self.fetch_all_jobs()
        with open(file_path, mode='w', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(['slug', 'language', 'req_id', 'title', 'description', 'city', 'state', 'postal_code', 'latitude', 'longitude', 'brand', 'employment_type', 'hiring_organization', 'apply_url', 'create_date'])
            writer.writerows(jobs)

    def close_connection(self):
        self.cursor.close()
        self.conn.close()

    def main(self):
        self.export_to_csv()
        self.close_connection()

if __name__ == "__main__":
    db_query = DatabaseQuery()
    db_query.main()