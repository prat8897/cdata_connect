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
    """
    def __init__(self, base_url: str = None, username: str = None,
                 password: str = None, config_path: str = None):
        if config_path:
            # Initialize connection from configuration file
            logger.debug("Connection - Initialising Connection with config")
            config = ConfigFactory.parse_file(config_path)
            self.base_url = config.get_string('cdata_api_db.base_url')
            self.auth = (config.get_string('cdata_api_db.username'),
                         config.get_string('cdata_api_db.password'))
        else:
            # Initialize connection from parameters
            logger.debug("Connection - Initialising Connection with username "
                         "and password")
            self.base_url = base_url
            self.auth = (username, password)

    def cursor(self):
        return Cursor(self)

    # Additional connection methods as required
