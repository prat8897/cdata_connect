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

## Multithreading
This library supports multithreading. You can make use of that like so:

```
import threading
from cdata_connect import Connection  # Adjust this import to your package structure

def thread_task():
    # Each thread creates its own connection
    conn = Connection(base_url= "https://cloud.cdata.com/api/",
                      username= "example@example.com",
                      password= "<your_personal_access_token_here>")
    cursor = conn.cursor()
    
    # Perform database operations using cursor
    cursor.execute("SELECT * FROM your_table")
    for row in cursor.fetchall():
        print(row)

# Create and start threads
threads = []
for i in range(5):  # Example: create 5 threads
    thread = threading.Thread(target=thread_task)
    threads.append(thread)
    thread.start()

# Wait for all threads to complete
for thread in threads:
    thread.join()
```