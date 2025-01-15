import struct
from influxdb_client.rest import ApiException
from influxdb_client import WritePrecision, InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timedelta
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

tz = 19800000  # Example timezone offset in milliseconds
token = "ZbE8G0q_BbeQjaq1ELHJ0GoqqznvZ7tFvhJZzqZbkSU5kIZ9iO-l62yRZj9k80YtsPHCtay6XzTQkbOVY04ntQ=="
org = "apropos"
bucket = "apt"
url = "http://3.18.152.95:8086"

class ErrorResponse(Exception):
    def __init__(self, message, status):
        super().__init__(message)
        self.status_code = status

def hex_to_float(hex_string):
    try:
        float_value = struct.unpack('<f', bytes.fromhex(hex_string))[0]
        return float_value
    except Exception as e:
        logger.error(f"Error converting hex {hex_string} to float: {e}")
        return None

def grp_by_twenty_per_metric(float_list):
    if len(float_list) != 200:
        raise ValueError(f"Incorrect data format: expected 200 float values but got {len(float_list)}")

    grouped = {f'v{i}': [] for i in range(20)}

    # Divide list into 10 groups of 20 values
    for i in range(10):
        group = float_list[i * 20:(i + 1) * 20]
        
        for j in range(20):
            grouped[f'v{j}'].append(group[j])

    #each v has exactly 10 values
    for i in range(20):
        if len(grouped[f'v{i}']) != 10:
            raise ValueError(f"Incorrect data format: {f'v{i}'} does not have 10 values")

    return grouped


def validate_and_format(data):
    logger.info("Starting validation and formatting")

    hex_string = data['D']
    if len(hex_string) < 20:
        raise ValueError('Hex string is too short to extract pic, date, and time')

    # set pic id (first 4 hex values)
    pic_id_hex = hex_string[0:4]
    data['pic'] = pic_id_hex
    print(f"PIC ID: {data['pic']}")
    logger.info(f"PIC ID: {data['pic']}")

    # Skip over the "00" at positions 4-5 (index 4-6) and continue extracting date and time
    try:
        day_hex = hex_string[6:8]
        month_hex = hex_string[8:10]
        year_hex = hex_string[10:14]
        hour_hex = hex_string[14:16]
        minute_hex = hex_string[16:18]
        second_hex = hex_string[18:20]

        day = int(day_hex, 16)
        month = int(month_hex, 16)
        year = int(year_hex, 16)
        hour = int(hour_hex, 16)
        minute = int(minute_hex, 16)
        second = int(second_hex, 16)

        # Adjust year
        if year < 100:
            year += 2000
        elif year < 1900:
            raise ValueError(f'Invalid year value: {year}')

        date_obj = datetime(year, month, day, hour, minute, second)
        data['converted_date_time'] = date_obj.isoformat()

        logger.info(f"Parsed Date and Time: {day}/{month}/{year} {hour}:{minute}:{second}")
        logger.info(f"Date Object: {date_obj}")

    except ValueError as e:
        logger.error(f"Error parsing date and time: {e}")
        raise

    # Convert hex strings to float values and group by 10
    float_values = []
    for i in range(20, len(hex_string), 8):
        hex_chunk = hex_string[i:i+8]
        if len(hex_chunk) == 8:
            float_value = hex_to_float(hex_chunk)
            if float_value is not None:
                float_values.append(float_value)
            else:
                logger.warning(f"Invalid hex chunk: {hex_chunk}")

    grouped = grp_by_twenty_per_metric(float_values)
    print(f"Grouped float values: {grouped}")

    for i in range(20):
        if len(grouped[f'v{i}']) != 10:
            raise ValueError(f"Incorrect data format: {f'v{i}'} does not have 10 values")

    return data, grouped, len(float_values) // 20

def format_and_write_to_db(data, grouped, time_value_len):
    arr = []
    date_obj_ist = datetime.fromisoformat(data['converted_date_time'])
    print(f"Original date_obj (IST): {date_obj_ist.isoformat()}")

    # Convert IST to UTC for InfluxDB
    date_obj_utc = date_obj_ist - timedelta(hours=5, minutes=30)
    epoch_d = date_obj_utc.timestamp() * 1000
    print(f"Epoch time in milliseconds (UTC): {epoch_d}")

    for i in range(20):
        values = grouped[f'v{i}']

        for k, value in enumerate(values):
            next_ten_milli_sec = (epoch_d + k * 100)
            timestamp = datetime.utcfromtimestamp(next_ten_milli_sec / 1000)
            formatted_date = timestamp.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            print(f"Generated timestamp for v{i}, value {k} (UTC): {formatted_date}")

            point = (
                Point(data['pic'])
                .tag('metric', f'v{i}')
                .field('value', value)
                .time(formatted_date, write_precision=WritePrecision.NS)
            )
            arr.append(point)
            print(f"Point added to InfluxDB batch: {point}")

    with InfluxDBClient(url=url, token=token, org=org) as client:
        try:
            with client.write_api(write_options=SYNCHRONOUS) as write_api:
                write_api.write(bucket=bucket, record=arr, time_precision="ns")
                logger.info('All data written successfully')
        except Exception as e:
            logger.error(f'Failed to write point: {e}')

    logger.info(f'Number of points written: {len(arr)}')
    return len(arr)


def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")

    if 'D' in event:
        event_data = event
    else:
        return {
            'statusCode': 400,
            'body': json.dumps('Invalid input: No D field found in the event')
        }

    try:
        data, grouped, time_value_len = validate_and_format(event_data)
        num_points_written = format_and_write_to_db(data, grouped, time_value_len)
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Data added! Number of points written: {num_points_written}',
                'date_obj': data['converted_date_time'],
                'grouped': grouped
            })
        }
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error processing data: {str(e)}')
        }