import requests
import ijson
import threading
from .log import logger
from .util.types import convert_to_python_type
from .exceptions import *


class Cursor:
    """
    Cursor class initialized by the Connection class.

    :init param connection: Base Connection class
    """
    def __init__(self, connection):
        self.connection = connection
        self._lock = threading.Lock()
        self.schema = None
        self.rows_generator = None
        self.current_row = 0
        self._rowcount = None

    def _check_connection(self):
        if not self.connection.is_open:
            raise Exception("Operation on closed connection is not allowed")

    @property
    def description(self):
        if self.schema:
            return [(column['columnName'], column['dataTypeName'],
                     None, None, None, None, None)
                    for column in self.schema]
        return None

    @property
    def rowcount(self):
        return self._rowcount

    def _execute_request(self, url, json_object):
        self._check_connection()
        try:
            with self._lock:
                logger.info("Cursor - Sending API request to "
                            f"{url} with json:"
                            f"{json_object}")

                self.response = requests.post(
                    url,
                    auth=self.connection.auth,
                    json=json_object,
                    stream=True)

                if self.response.status_code == 200:
                    self.json_reader = ijson.parse(self.response.raw)
                    self._process_schema()
                    self._prepare_rows_reader()
                else:
                    error_string = f"Cursor - API request failed with status "\
                                f"code {self.response.status_code}:"\
                                f"{self.response.text}"
                    logger.error(error_string)
                    raise Exception(error_string)

        except requests.exceptions.RequestException as e:
            raise OperationalError(f"Error executing query: {str(e)}") from e

        except ijson.JSONError as e:
            raise DataError(f"Error parsing JSON response: {str(e)}") from e

        except Exception as e:
            raise DatabaseError(f"Unexpected error occurred: {str(e)}") from e

    def _process_schema(self):
        self.schema = []
        for prefix, event, value in self.json_reader:
            if prefix == 'results.item.schema.item' and event == 'start_map':
                current_schema_item = {}
            elif prefix == 'results.item.schema.item' and event == 'end_map':
                self.schema.append(current_schema_item)
                if len(self.schema) == len(self.description):
                    break
            elif prefix.startswith('results.item.schema.item.'):
                key = prefix.split('.')[-1]
                current_schema_item[key] = value

    def _prepare_rows_reader(self):
        self.rows_generator = ijson.items(self.json_reader,
                                          'results.item.rows.item')

    def execute(self, query: str, params: dict = None):
        self._check_connection()
        self._rowcount = None
        if params is not None and not isinstance(params, dict):
            raise ValueError("Params must be a dictionary")

        if params:
            query = query % params

        json_object = {"query": query}
        if self.connection.workspace:
            base_url = f"{self.connection.base_url}/query?workspace="\
                       f"{self.connection.workspace}"
        else:
            base_url = f"{self.connection.base_url}/query"
        self._execute_request(base_url, json_object)

    def executemany(self, query: str, schema: str, params: list = None):
        self._check_connection()
        self._rowcount = None
        json_object = {"query": query,
                       "defaultSchema": schema,
                       "parameters": params}
        if self.connection.workspace:
            base_url = f"{self.connection.base_url}/batch?workspace="\
                       f"{self.connection.workspace}"
        else:
            base_url = f"{self.connection.base_url}/batch"
        self._execute_request(f"{base_url}", json_object)

    def callproc(self, procedure: str, schema: str, params: dict = None):
        self._check_connection()
        self._rowcount = None
        if params is not None and not isinstance(params, dict):
            raise ValueError("Params must be a dictionary")

        json_object = {"procedure": procedure,
                       "defaultSchema": schema,
                       "parameters": params}
        if self.connection.workspace:
            base_url = f"{self.connection.base_url}/exec?workspace="\
                       f"{self.connection.workspace}"
        else:
            base_url = f"{self.connection.base_url}/exec"
        self._execute_request(f"{base_url}", json_object)

    def _convert_row(self, row):
        data_type_names = [schema_item['dataTypeName'] for
                           schema_item in self.schema]
        converted_row = [convert_to_python_type(value, data_type_name) for
                         data_type_name, value in zip(data_type_names, row)]
        return converted_row

    def close(self):
        self._check_connection()
        # Close the response stream if it exists and hasn't been consumed
        if self.response is not None:
            try:
                self.response.close()
            except Exception as e:
                logger.warning(f"Error closing response stream: {str(e)}")
            finally:
                self.response = None

        # Reset the connection state
        self.connection = None

    def fetchone(self):
        self._check_connection()
        try:
            current_row = next(self.rows_generator)
            processed_row = self._convert_row(current_row)
            return processed_row
        except StopIteration:
            return None

    def fetchall(self):
        self._check_connection()
        processed_rows = []
        for row in self.rows_generator:
            processed_row = [self._convert_row([value])[0] for value in row]
            processed_rows.append(processed_row)
        return processed_rows

    def fetchmany(self, size=None):
        self._check_connection()
        if size is None:
            size = self.arraysize

        rows = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows

    @property
    def arraysize(self):
        # Default arraysize if not set
        return getattr(self, '_arraysize', 1)

    @arraysize.setter
    def arraysize(self, value):
        self._arraysize = value
