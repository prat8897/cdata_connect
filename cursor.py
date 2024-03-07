import requests
import ijson
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
        self.rows = []
        self.current_row = 0
        self._rows_generator = None

    def execute(self, query: str, params: dict = None):

        if params is not None and not isinstance(params, dict):
            raise ValueError("Params must be a dictionary")

        if params:
            query = query % params

        json_object = {"query": query}
        logger.info("Cursor - Sending API request to "
                    f"{self.connection.base_url} with json: {json_object}")

        self.response = requests.post(f"{self.connection.base_url}/query",
                                      auth=self.connection.auth,
                                      json=json_object,
                                      stream=True)

        # Initialize the rows and schema as empty lists
        self.rows = []
        self.schema = []

        if self.response.status_code == 200:

            # Use ijson to parse the response incrementally
            parser = ijson.parse(self.response.raw)
            prefix, event, value = next(parser)

            # Assuming the first relevant key in the JSON is 'results.item'
            while not (prefix == 'results.item' and event == 'start_map'):
                prefix, event, value = next(parser)

            # Process the JSON elements inside 'results.item'
            inside_schema = False
            for prefix, event, value in parser:
                if event == 'start_map' and 'schema' in prefix:
                    inside_schema = True
                elif event == 'end_map' and inside_schema:
                    inside_schema = False
                elif inside_schema and event in ['string', 'number']:
                    self.schema.append((prefix, value))
                elif 'rows.item' in prefix and event == 'start_array':
                    self.rows.append([])
                elif prefix.endswith('.item') and event in \
                        ['string', 'number']:
                    self.rows[-1].append(value)

        else:
            error_string = f"Cursor - API request failed with status code\
                    {self.response.status_code}: {self.response.text}"
            logger.error(error_string)
            raise Exception(error_string)

        return self

    def executemany(self, query: str, schema: str, params: list = None):

        json_object = {"query": query,
                       "defaultSchema": schema,
                       "parameters": params}

        logger.info("Cursor - Sending API request to "
                    f"{self.connection.base_url} with json: {json_object}")

        self.response = requests.post(f"{self.connection.base_url}/batch",
                                      auth=self.connection.auth,
                                      json=json_object,
                                      stream=True)

        # Initialize the rows and schema as empty lists
        self.rows = []
        self.schema = []

        if self.response.status_code == 200:

            # Use ijson to parse the response incrementally
            parser = ijson.parse(self.response.raw)
            prefix, event, value = next(parser)

            # Assuming the first relevant key in the JSON is 'results.item'
            while not (prefix == 'results.item' and event == 'start_map'):
                prefix, event, value = next(parser)

            # Process the JSON elements inside 'results.item'
            inside_schema = False
            for prefix, event, value in parser:
                if event == 'start_map' and 'schema' in prefix:
                    inside_schema = True
                elif event == 'end_map' and inside_schema:
                    inside_schema = False
                elif inside_schema and event in ['string', 'number']:
                    self.schema.append((prefix, value))
                elif 'rows.item' in prefix and event == 'start_array':
                    self.rows.append([])
                elif prefix.endswith('.item') and event in\
                        ['string', 'number']:
                    self.rows[-1].append(value)

        else:
            error_string = f"Cursor - API request failed with status code\
                    {self.response.status_code}: {self.response.text}"
            logger.error(error_string)
            raise Exception(error_string)

        return self

    def callproc(self, procedure: str, schema: str, params: dict = None):

        if params is not None and not isinstance(params, dict):
            raise ValueError("Params must be a dictionary")

        json_object = {"procedure": procedure,
                       "defaultSchema": schema,
                       "parameters": params}
        logger.info("Cursor - Sending API request to "
                    f"{self.connection.base_url} with json: {json_object}")

        self.response = requests.post(f"{self.connection.base_url}/query",
                                      auth=self.connection.auth,
                                      json=json_object,
                                      stream=True)

        # Initialize the rows and schema as empty lists
        self.rows = []
        self.schema = []

        if self.response.status_code == 200:

            # Use ijson to parse the response incrementally
            parser = ijson.parse(self.response.raw)
            prefix, event, value = next(parser)

            # Assuming the first relevant key in the JSON is 'results.item'
            while not (prefix == 'results.item' and event == 'start_map'):
                prefix, event, value = next(parser)

            # Process the JSON elements inside 'results.item'
            inside_schema = False
            for prefix, event, value in parser:
                if event == 'start_map' and 'schema' in prefix:
                    inside_schema = True
                elif event == 'end_map' and inside_schema:
                    inside_schema = False
                elif inside_schema and event in ['string', 'number']:
                    self.schema.append((prefix, value))
                elif 'rows.item' in prefix and event == 'start_array':
                    self.rows.append([])
                elif prefix.endswith('.item') and event in\
                        ['string', 'number']:
                    self.rows[-1].append(value)

        else:
            error_string = f"Cursor - API request failed with status code\
                    {self.response.status_code}: {self.response.text}"
            logger.error(error_string)
            raise Exception(error_string)

        return self

    def _convert_row(self, row):
        data_type_names = [value for key, value in self.schema
                           if 'dataTypeName' in key]

        converted_row = [convert_to_python_type(value, data_type_name)
                         for data_type_name, value in
                         zip(data_type_names, row)]
        return converted_row

    def fetchone(self):
        if not self.rows:
            return None
        return self._convert_row(self.rows.pop(0))

    def fetchall(self):
        # This consumes the iterator and gathers the rest of the items
        rows = list([self._convert_row(row) for row in self.rows])
        return rows

    def fetchmany(self, size: int = 1):
        rows = []
        for _ in range(size):
            try:
                rows.append(self._convert_row(self.rows.pop(0)))
            except StopIteration:
                break  # Stop if there are no more items
        return rows
