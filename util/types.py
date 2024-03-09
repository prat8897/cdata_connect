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
    elif data_type == 'NULL':
        return None
    else:
        raise ValueError(f"Unsupported data type: {data_type}")


class Date:
    def __init__(self, year, month, day):
        self.year = year
        self.month = month
        self.day = day

    @classmethod
    def fromtimestamp(cls, timestamp):
        date = datetime.fromtimestamp(timestamp).date()
        return cls(date.year, date.month, date.day)


class Time:
    def __init__(self, hour, minute, second):
        self.hour = hour
        self.minute = minute
        self.second = second

    @classmethod
    def fromtimestamp(cls, timestamp):
        time = datetime.fromtimestamp(timestamp).time()
        return cls(time.hour, time.minute, time.second)


class Timestamp:
    def __init__(self, year, month, day, hour, minute, second):
        self.year = year
        self.month = month
        self.day = day
        self.hour = hour
        self.minute = minute
        self.second = second

    @classmethod
    def fromtimestamp(cls, timestamp):
        dt = datetime.fromtimestamp(timestamp)
        return cls(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)


def DateFromTicks(ticks):
    return Date.fromtimestamp(ticks)


def TimeFromTicks(ticks):
    return Time.fromtimestamp(ticks)


def TimestampFromTicks(ticks):
    return Timestamp.fromtimestamp(ticks)


class Binary:
    def __init__(self, data):
        self.data = data
