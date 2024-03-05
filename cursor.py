import requests
import json
from .log import logger
import ijson
from .util.types import convert_to_python_type


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
            json=json_object,
            stream=True
        )
        if response.status_code != 200:
            error_string = "Cursor - API request failed with status code "\
                           f"{response.status_code}: {response.text}"
            logger.error(error_string)
            raise Exception(error_string)     
        self.row_generator = self._row_generator(response.raw)

        logger.info(f"Cursor - Received response: {response.text}")
        data = json.loads(response.text)
        self.schema = data['results'][0]['schema']
        self.rows = data['results'][0]['rows']
        self.current_row = 0
        return data

    def _row_generator(self, raw_response):
        items = ijson.items(raw_response, 'results.item.rows.item')
        for item in items:
            yield item

    def _convert_row(self, row):
        converted_row = []
        for idx, value in enumerate(row):
            data_type_name = self.schema[idx]['dataTypeName']
            converted_row.append(convert_to_python_type(value, data_type_name))
        return converted_row

    def fetchone(self):
        if self.row_generator:
            try:
                raw_row = next(self.row_generator)
                return self._convert_row(raw_row)
            except StopIteration:
                return None

    def fetchall(self):
        all_rows = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            all_rows.append(row)
        return all_rows

    def fetchmany(self, size: int = 1):
        many_rows = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            many_rows.append(row)
        return many_rows

    
