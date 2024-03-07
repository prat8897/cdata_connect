## Overview

The Python Connector for CData Connect allows developers to write Python scripts with connectivity to CData Connect. The connector wraps the complexity of accessing CData Connect data in an interface commonly used by python connectors to common database systems.

## Establishing a Connection
The objects available within our connector are accessible from the "cdata_connect" module. In order to use the module's objects directly, the module must first be imported.Then, you can establish a connection in one of two ways:

```
from cdata_connect import Connection
conn = Connection(base_url= "https://cloud.cdata.com/api/",
                  username= "example@example.com",
                  password= "<your_personal_access_token_here>")
```
```
import cdata_connect as mod
conn = mod.connect(base_url= "https://cloud.cdata.com/api/",
                  username= "example@example.com",
                  password= "<your_personal_access_token_here>")
```
## PyHOCON Config file
This library supports PyHOCON formatted config files to pass as a parameter for Connection the the CData cloud. 
An example `config.conf` can look like:

```
cdata_api_db {
  base_url = "https://cloud.cdata.com/api/"
  username = "example@example.com"
  password = "<your_personal_access_token_here>"
}
```

You can then pass this config file as a parameter to the Connection class like so:

```
from cdata_connect import Connection
conn = Connection(config_path="config.conf")
```
or
```
import cdata_connect as mod
conn = mod.connect(config_path="config.conf")
```

## Cursor
After establishing a connection, you can create a cursor like so:
```
cursor = conn.cursor()
```

You can then make queries like so:
```
query = """SELECT * FROM [Test1].[OData].[Airlines]"""
cursor.execute(query)
result = cursor.fetchall()
for row in result:
    print(row)
```

### Parameterised Queries
This library supports the Pyformat protocol for making parameterised queries. You can make these queries with a `params` dictionary like so:
```
params = {
    "id": 123,
    "name": "John Doe"
}

query = "SELECT * FROM users WHERE id = %(id)s AND name = %(name)s"
cursor.execute(query, params)
```
## Batch Operation
The batch operation allows you to execute batch INSERTs, UPDATEs, and DELETEs against your data sources using a single request.

To execute a batch operation,

```
query = "SELECT * FROM users WHERE id = %(id)s AND name = %(name)s"
schema = "Schema_Name"
params = [
    {
      "@p1": { "dataType": <int>, "value": <any> },
      "@p2": { "dataType": <int>, "value": <any> },
      ...
    },
    ...
  ]
cursor.executemany(query, schema, params)
```
## Execute Procedures
The execute operation allows you to execute stored procedures for any of the data sources configured in your CData Connect account.

```
procedure = "Stored_Procedure"
schema = "Schema_Name"
params = {
    "@p1": { "dataType": <int>, "direction": <int>, "value": <any> },
    "@p2": { "dataType": <int>, "direction": <int>, "value": <any> },
    ...
  }
cursor.callproc(procedure, schema, params)
```


## Multithreading
This library supports multithreading. You can make use of that like so:
```
from cdata_connect.thread_safety import ThreadSafeConnectionHandler
import threading

def execute_query(query):
    handler = ThreadSafeConnectionHandler()
    connection = handler.get_connection(base_url="https://cloud.cdata.com/api/",
                                        username="example@example.com",
                                        password="<your_personal_access_token_here>",
                                        operation_type='query')
    cursor = connection.cursor()
    try:
        result = cursor.execute(query)
        for row in cursor.fetchall():
            print(row)  # Or process your result as needed
    except Exception as e:
        print(f"Query execution error: {e}")

def start_threads():
    # List of queries to execute
    queries = [
        "SELECT * FROM table1",
        "SELECT * FROM table2",
        "UPDATE table1 SET column1 = value WHERE condition",
    ]

    threads = []

    for query in queries:
        thread = threading.Thread(target=execute_query, args=(query,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

if __name__ == "__main__":
    start_threads()
```
