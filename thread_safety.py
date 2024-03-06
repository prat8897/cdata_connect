import threading
from .connection import Connection


class ThreadSafeConnectionHandler:
    def __init__(self):
        # Thread-local storage to maintain separate connection for each thread
        self._thread_local_storage = threading.local()

    def get_connection(self, *args, **kwargs):
        """
        Retrieve a thread-local Connection instance. If a connection does not
        exist for the current thread, create a new one using the provided
        arguments.

        :param args: Arguments passed to the Connection constructor.
        :param kwargs: Keyword arguments passed to the Connection constructor.
        :return: A thread-local Connection instance.
        """
        if not hasattr(self._thread_local_storage, 'connection'):
            # Initialize the Connection for this thread
            self._thread_local_storage.connection = Connection(*args, **kwargs)
        return self._thread_local_storage.connection


"""
# Example Usage
handler = ThreadSafeConnectionHandler()
connection = handler.get_connection(base_url='https://api.example.com',
                                    username='user', password='pass')
"""
