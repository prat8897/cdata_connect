from datetime import datetime, date, time
from decimal import Decimal
import uuid

def convert_to_python_type(value, data_type):
    if data_type in ['Tinyint', 'Smallint', 'Integer', 'Bigint']:
        return int(value)
    elif data_type in ['Float', 'Double']:
        return float(value)
    elif data_type in ['Decimal', 'Numeric']:
        return Decimal(value)
    elif data_type == 'Boolean':
        return value.lower() in ['true', '1']
    elif data_type == 'Date':
        return datetime.strptime(value, '%Y-%m-%d').date()
    elif data_type == 'Time':
        return datetime.strptime(value, '%H:%M:%S').time()
    elif data_type == 'Timestamp':
        return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    elif data_type == 'UUID':
        return uuid.UUID(value)
    elif data_type == 'Binary':
        return bytes(value, 'utf-8')
    elif data_type == 'Varchar':
        return str(value)
    else:
        raise ValueError(f"Unsupported data type: {data_type}")

# Example usage
value = '2024-01-31'
data_type = 'Date'
converted_value = convert_to_python_type(value, data_type)
print(f"Original: {value} ({data_type}), Converted: {converted_value} ({type(converted_value)})")
