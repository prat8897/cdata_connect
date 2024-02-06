import requests
import json
from .log import logger


class Cursor:
    """
    Cursor class initialized by the Connection class.

    :init param connection: Base Connection class
    """
    def __init__(self, connection):
        self.connection = connection
        self.schema = None
        self.rows = None
        self.current_row = 0

    def execute(self, query: str):
        json_object = {"query": query}
        logger.info("Cursor - Sending API request to "
                    f"{self.connection.base_url} with json: {json_object}")
        response = requests.post(
            f"{self.connection.base_url}",
            auth=self.connection.auth,
            json=json_object
        )
        if response.status_code != 200:
            error_string = "Cursor - API request failed with status code "\
                           f"{response.status_code}: {response.text}"
            logger.error(error_string)
            raise Exception(error_string)

        logger.info(f"Cursor - Received response: {response.text}")
        data = json.loads(response.text)
        self.schema = data['results'][0]['schema']
        self.rows = data['results'][0]['rows']
        self.current_row = 0
        return data

    def fetchone(self):
        if not self.rows:
            return None

        if self.current_row >= len(self.rows):
            return None

        row = self.rows[self.current_row]
        self.current_row += 1
        return row

    def fetchall(self):
        if not self.rows:
            return []

        remaining_rows = self.rows[self.current_row:]
        self.current_row = len(self.rows)
        return remaining_rows

    def fetchmany(self, size: int = 1):
        if not self.rows:
            return []

        end_row = min(self.current_row + size, len(self.rows))
        rows = self.rows[self.current_row:end_row]
        self.current_row = end_row
        return rows

    # Additional methods as required
