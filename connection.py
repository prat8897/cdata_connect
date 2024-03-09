from pyhocon import ConfigFactory
import threading
from .cursor import Cursor
from .log import logger
from .exceptions import *


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
        self.is_open = True

        if workspace is not None:
            self.workspace = workspace
        else:
            self.workspace = None

        if config_path:
            # Initialize connection from configuration file
            logger.debug("Connection - Initialising Connection with config")
            config = ConfigFactory.parse_file(config_path)

            try:
                config_base_url = config.get_string('cdata_api_db.base_url')
                self.base_url = config_base_url.rstrip('/')
                self.auth = (config.get_string('cdata_api_db.username'),
                             config.get_string('cdata_api_db.password'))
            except FileNotFoundError as e:
                raise ConfigurationError(f"Missing required configuration:"
                                         f"{str(e)}") from e

        else:
            # Initialize connection from parameters
            logger.debug("Connection - Initialising Connection with "
                         "username and password")

            if base_url is None or username is None or password is None:
                raise ConfigurationError("Missing required parameters:" 
                                         "base_url, username, and password "
                                         "must be provided.")

            self.base_url = base_url.rstrip('/')
            self.auth = (username, password)

    def commit(self):
        """Commit any pending transaction to the database.
        Since the underlying system does not support transactions,
        this method is implemented as a no-op.
        """

        pass

    def rollback(self):
        """This method is a no-op as the underlying system does not
        support transactions."""

        pass

    def close(self):
        self.is_open = False

    def cursor(self):
        if not hasattr(self._local, 'cursor'):
            self._local.cursor = Cursor(self)
        return self._local.cursor
