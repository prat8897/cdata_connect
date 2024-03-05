from datetime import datetime
from decimal import Decimal
import uuid


def convert_to_python_type(value, data_type):
    if data_type in ['TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT']:
        return int(value)
    elif data_type in ['FLOAT', 'DOUBLE']:
        return float(value)
    elif data_type in ['DECIMAL', 'NUMERIC']:
        return Decimal(value)
    elif data_type == 'BOOLEAN':
        return value.lower() in ['true', '1']
    elif data_type == 'DATE':
        return datetime.strptime(value, '%Y-%m-%d').date()
    elif data_type == 'TIME':
        return datetime.strptime(value, '%H:%M:%S').time()
    elif data_type == 'TIMESTAMP':
        return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    elif data_type == 'UUID':
        return uuid.UUID(value)
    elif data_type == 'BINARY':
        return bytearray(value)
    elif data_type == 'VARCHAR':
        return str(value)
    else:
        raise ValueError(f"Unsupported data type: {data_type}")


"""
# Example usage
value = '2024-01-31'
data_type = 'Date'
converted_value = convert_to_python_type(value, data_type)
print(f"Original: {value} ({data_type}), Converted: {converted_value}"
       "({type(converted_value)})")
"""
