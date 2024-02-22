from .connection import Connection


def connect(config_path: str = None, base_url: str = None,
            username: str = None, password: str = None):
    """
    Create a connection to the CData Connect API.

    :param base_url: The base URL of the CData Connect API.
    :param username: The username for authentication.
    :param password: The password for authentication.
    :param config_path: Optional path to a config file in pyhocon format.
    :return: A Connection object.
    """
    if config_path:
        return Connection(config_path)
    elif base_url and username and password:
        return Connection(base_url, username, password)
    else:
        raise ValueError("Either config_path or base_url, username,"
                         "and password must be provided.")


apilevel = "2.0"
threadsafety = 1
paramstyle = "pyformat"
