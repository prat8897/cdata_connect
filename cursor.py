import requests
import json
from .log import logger
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

    def execute(self, query: str, params: dict = None):

        if params is not None and not isinstance(params, dict):
            raise ValueError("Params must be a dictionary")

        if params:
            query = query % params

        json_object = {"query": query}
        logger.info("Cursor - Sending API request to "
                    f"{self.connection.base_url} with json: {json_object}")
        response = requests.post(
            self.connection.base_url,
            auth=self.connection.auth,
            json=json_object,
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
        self.rowcount = data['results'][0]['affectedRows']
        self.current_row = 0

        self.row_generator = (row for row in self.rows)
        return self

    def _convert_row(self, row):
        converted_row = []
        for idx, value in enumerate(row):
            data_type_name = self.schema[idx]['dataTypeName']
            converted_row.append(convert_to_python_type(value, data_type_name))
        return converted_row

    def fetchone(self):
        if self.current_row < len(self.rows):
            row = self.rows[self.current_row]
            self.current_row += 1
            return self._convert_row(row)
        return None

    def fetchall(self):
        remaining_rows = self.rows[self.current_row:]
        self.current_row = len(self.rows)  # Move the cursor to the end
        return [self._convert_row(row) for row in remaining_rows]

    def fetchmany(self, size=1):
        end_row = self.current_row + size
        rows = self.rows[self.current_row:end_row]
        self.current_row = end_row
        return [self._convert_row(row) for row in rows]
