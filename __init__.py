from .connection import Connection


def connect(config_path: str = None, base_url: str = None,
            username: str = None, password: str = None,
            operation_type: str = None, workspace: str = None):
    """
    Create a connection to the CData Connect API.

    :param base_url: The base URL of the CData Connect API.
    :param username: The username for authentication.
    :param password: The password for authentication.
    :param config_path: Optional path to a config file in pyhocon format.
    :init param workspace: Optional arg to access a specific CData workspace.
    :return: A Connection object.
    """
    if not workspace:
        if config_path:
            return Connection(config_path=config_path,
                              operation_type=operation_type)
        elif base_url and username and password:
            return Connection(base_url=base_url, username=username,
                              password=password, operation_type=operation_type)
        else:
            raise ValueError("Either config_path or base_url, username,"
                             "and password must be provided.")
    elif workspace:
        if config_path:
            return Connection(config_path=config_path,
                              operation_type=operation_type)
        elif base_url and username and password:
            workspace_url = f"{base_url}?workspace={workspace}"
            return Connection(base_url=workspace_url, username=username,
                              password=password, operation_type=operation_type)
        else:
            raise ValueError("Either config_path or base_url, username,"
                             "and password must be provided.")


apilevel = "2.0"
threadsafety = 1
paramstyle = "pyformat"
