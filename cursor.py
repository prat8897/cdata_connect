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

        # Initialize variables
        self.rows = []
        self.schema = []
        self.rowcount = None  # Reset to None every time execute is called

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
                    self.rowcount = value

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

        # Initialize variables
        self.rows = []
        self.schema = []
        self.rowcount = None  # Reset to None every time execute is called

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
                    self.rowcount = value

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

        # Initialize variables
        self.rows = []
        self.schema = []
        self.rowcount = None  # Reset to None every time execute is called

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
                    self.rowcount = value

        else:
            error_string = f"Cursor - API request failed with status code\
                    {self.response.status_code}: {self.response.text}"
            logger.error(error_string)
            raise Exception(error_string)

        return self

    def _convert_row(self, row):
        # Extract 'dataTypeName' values from each dictionary in self.schema
        data_type_names = [schema_item['dataTypeName'] for
                           schema_item in self.schema]

        # Assuming convert_to_python_type is a function 
        # you have defined elsewhere that converts each value to 
        # the correct Python type based on data_type_name
        converted_row = [convert_to_python_type(value, data_type_name)
                         for data_type_name, value in zip(data_type_names, row)]
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
            except IndexError:
                break  # Stop if there are no more items
        return rows
