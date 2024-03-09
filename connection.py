from pyhocon import ConfigFactory
import threading
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
                 workspace: str = None):

        self._local = threading.local()

        if config_path:
            # Initialize connection from configuration file
            logger.debug("Connection - Initialising Connection"
                         "with config")
            config = ConfigFactory.parse_file(config_path)
            config_base_url = config.get_string('cdata_api_db.base_url')
            self.base_url = config_base_url.rstrip('/')

            if workspace is not None:
                self.base_url += f"?workspace={workspace}"

            self.auth = (config.get_string('cdata_api_db.username'),
                         config.get_string('cdata_api_db.password'))
        else:
            # Initialize connection from parameters
            logger.debug("Connection - Initialising Connection with"
                         "username and password")
            self.base_url = base_url.rstrip('/')

            if workspace is not None:
                self.base_url += f"?workspace={workspace}"

            self.auth = (username, password)

    def cursor(self):
        if not hasattr(self._local, 'cursor'):
            self._local.cursor = Cursor(self)
        return self._local.cursor
