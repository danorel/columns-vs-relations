import psycopg2

from typing import List

from libs.shared.server import app
from libs.shared.db import DatabaseGenerator


class DatabaseManager:
    def __init__(self,
                 application: str,
                 tables_metadata: List[dict],
                 generator: DatabaseGenerator):
        self.application = application
        self.generator = generator
        try:
            self.connection = psycopg2.connect(
                host='postgres',
                port=3123
            )
            self.cursor = self.connection.cursor()
        except Exception as e:
            app.logger.critical(f"Connection error: {e}")
            raise RuntimeError(f"Connection error: {e}")

        self.tables_metadata: List[dict] = tables_metadata
        try:
            for table_metadata in tables_metadata:
                name = table_metadata.get('name')
                families = table_metadata.get('families')
                self.connection.create_table(
                    name=name,
                    families=families
                )
        except Exception as e:
            app.logger.error(f"Table creation error: {e}")

    def generate_data(self):
        for table_metadata in self.tables_metadata:
            table = self.connection.table(name=table_metadata.get('name'))
            data = self.generator.random_data(
                column_family=list(table_metadata.get('families').keys()).pop(),
                columns=table_metadata.get('columns')
            )
            batch = table.batch()
            try:
                for i, entry in enumerate(data):
                    key = str.encode(str(i))
                    batch.put(key, entry)
            except Exception as e:
                app.logger.error(f"Send error: {e}")
            else:
                batch.send()

    def __del__(self):
        try:
            for table_metadata in self.tables_metadata:
                name = table_metadata.get('name')
                self.connection.disable_table(name)
                self.connection.delete_table(name)
            self.connection.close()
        except Exception as e:
            app.logger.error(f"Table removal error: {e}")

    def __repr__(self):
        return f"Tables: {self.connection.tables()}"
