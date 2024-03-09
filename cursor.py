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
        self.rows = []
        self.current_row = 0
        self._rowcount = None

    @property
    def description(self):
        if self.schema:
            return [(column['columnName'], column['dataTypeName'],
                     None, None, None, None, None)
                    for column in self.schema]
        return None

    def rowcount(self):
        return self._rowcount
    
    def _execute_request(self, url, json_object):
        try:
            with self._lock:
                logger.info("Cursor - Sending API request to "
                            f"{self.connection.base_url} with json:"
                            f"{json_object}")

                self.response = requests.post(
                    url,
                    auth=self.connection.auth,
                    json=json_object,
                    stream=True)

                # Initialize variables
                self.rows = []
                self.schema = []
                # Reset to None every time execute is called
                self.rowcount = None

                if self.response.status_code == 200:
                    # Parsing the JSON stream
                    parser = ijson.parse(self.response.raw)

                    # Variables to help navigate the JSON structure
                    inside_schema = False
                    inside_rows = False

                    for prefix, event, value in parser:
                        if prefix == 'results.item.schema.item' and \
                                event == 'start_map':
                            # Start of a new schema item
                            inside_schema = True
                            current_schema_item = {}
                        elif prefix == 'results.item.schema.item' and \
                                event == 'end_map':
                            # End of the current schema item
                            inside_schema = False
                            self.schema.append(current_schema_item)
                        elif inside_schema:
                            # Collect schema information
                            # Get the last part of the prefix as the key
                            key = prefix.split('.')[-1]
                            current_schema_item[key] = value
                        elif prefix == 'results.item.rows.item' and \
                                event == 'start_array':
                            # Start of a new row
                            inside_rows = True
                            current_row = []
                        elif prefix == 'results.item.rows.item' and \
                                event == 'end_array':
                            # End of the current row
                            inside_rows = False
                            self.rows.append(current_row)
                        elif inside_rows and event in ['string', 'number']:
                            # Append values to the current row
                            current_row.append(value)
                        elif prefix == 'results.item.affectedRows':
                            # Store the affectedRows value
                            self._rowcount = value

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

        finally:
            self.close()

    def execute(self, query: str, params: dict = None):
        self._rowcount = None
        if params is not None and not isinstance(params, dict):
            raise ValueError("Params must be a dictionary")

        if params:
            query = query % params

        json_object = {"query": query}
        self._execute_request(f"{self.connection.base_url}/query", json_object)

    def executemany(self, query: str, schema: str, params: list = None):
        self._rowcount = None
        json_object = {"query": query,
                    "defaultSchema": schema,
                    "parameters": params}
        self._execute_request(f"{self.connection.base_url}/batch", json_object)

    def callproc(self, procedure: str, schema: str, params: dict = None):
        self._rowcount = None
        if params is not None and not isinstance(params, dict):
            raise ValueError("Params must be a dictionary")

        json_object = {"procedure": procedure,
                    "defaultSchema": schema,
                    "parameters": params}
        self._execute_request(f"{self.connection.base_url}/query", json_object)

    def _convert_row(self, row):
        # Extract 'dataTypeName' values from each dictionary in self.schema
        data_type_names = [schema_item['dataTypeName'] for
                           schema_item in self.schema]

        converted_row = [convert_to_python_type(value, data_type_name) for
                         data_type_name, value in zip(data_type_names, row)]
        return converted_row

    def close(self):
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
        if not self.rows:
            return None
        return self._convert_row(self.rows.pop(0))

    def fetchall(self):
        # This consumes the iterator and gathers the rest of the items
        rows = list([self._convert_row(row) for row in self.rows])
        return rows

    def fetchmany(self, size: int = 1):
        if size is None:
            size = self.arraysize

        rows = []
        for _ in range(size):
            try:
                rows.append(self._convert_row(self.rows.pop(0)))
            except IndexError:
                break  # Stop if there are no more items
        return rows

    @property
    def arraysize(self):
        # Default arraysize if not set
        return getattr(self, '_arraysize', 1)

    @arraysize.setter
    def arraysize(self, value):
        self._arraysize = value
