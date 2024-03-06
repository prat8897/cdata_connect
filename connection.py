from pyhocon import ConfigFactory
from .cursor import Cursor
from .log import logger


class Connection:
    """
    Create a connection to the CData Connect API.
    :init param base_url: The base URL of the CData Connect API.
    :init param username: The username for authentication.
    :init param password: The password for authentication.
    :init param config_path: Optional path to a config file in pyhocon format.
    :init param workspace: Optional arg to access a specific CData workspace.
    """
    def __init__(self, base_url: str = None, username: str = None,
                 password: str = None, config_path: str = None,
                 operation_type: str = None, workspace: str = None):

        endpoint_map = {
            'query': '/query',
            'batch': '/batch',
            'exec': '/exec'
        }

        if config_path:
            # Initialize connection from configuration file
            logger.debug("Connection - Initialising Connection"
                         "with config")
            config = ConfigFactory.parse_file(config_path)
            config_base_url = config.get_string('cdata_api_db.base_url')
            self.base_url = config_base_url.rstrip('/') + \
                endpoint_map.get(operation_type, '/query')

            if workspace is not None:
                self.base_url += f"?workspace={workspace}"

            self.auth = (config.get_string('cdata_api_db.username'),
                         config.get_string('cdata_api_db.password'))
        else:
            # Initialize connection from parameters
            logger.debug("Connection - Initialising Connection with"
                         "username and password")
            self.base_url = base_url.rstrip('/') + \
                endpoint_map.get(operation_type, '/query')

            if workspace is not None:
                self.base_url += f"?workspace={workspace}"

            self.auth = (username, password)

    def cursor(self):
        return Cursor(self)
