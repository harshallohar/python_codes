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
token = "ZbE8G0q_BbeQjaq1ELHJ0GoqqznvZ7tFvhJZzqZbkSU5kIZ9iO-l62yRZj9k80YtsPHCtay6XzTQkbOVY04ntQ=="   # InfluxDB API token
org = "apropos"   # InfluxDB organization name
bucket = "apt"    # InfluxDB bucket name
url = "http://3.18.152.95:8086"    # URL of the InfluxDB instance

class ErrorResponse(Exception):
    def __init__(self, message, status):
        super().__init__(message)
        self.status_code = status

#Converts a hexadecimal string to a floating-point number.
def hex_to_float(hex_string):
    try:
        float_value = struct.unpack('<f', bytes.fromhex(hex_string))[0]
        return float_value
    except Exception as e:
        logger.error(f"Error converting hex {hex_string} to float: {e}")
        return None

#Groups a list of floating-point numbers into groups of 20.
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

#Validates and formats the input data
def validate_and_format(data):
    logger.info("Starting validation and formatting")

    hex_string = data['D']
    if len(hex_string) < 24:
        raise ValueError('Hex string is too short to extract pic, date, and time')

    # Extract PIC ID
    pic_id_hex = hex_string[0:8]
    data['pic'] = pic_id_hex  # Convert the extracted hex string to an integer
    print(f"PIC ID: {data['pic']}")
    logger.info(f"PIC ID: {data['pic']}")

    # Skip over the "00" at positions 8-9 and continue extracting date and time
    try:
        year_hex = hex_string[10:14]
        month_hex = hex_string[14:16]  	# Extract day (2 hex characters)
        day_hex = hex_string[16:18]
        hour_hex = hex_string[18:20]
        minute_hex = hex_string[20:22]
        second_hex = hex_string[22:24]

        # Convert extracted hex strings to integers (base 16)
        day = int(day_hex, 16)
        month = int(month_hex, 16)
        year = int(year_hex, 16)
        hour = int(hour_hex, 16)
        minute = int(minute_hex, 16)
        second = int(second_hex, 16)

        # Validate month
        if not (1 <= month <= 12):
            raise ValueError(f'Invalid month value: {month}')

        
        # Adjust year
        if year < 100:
            year += 2000
        elif year < 1900:
            raise ValueError(f'Invalid year value: {year}')
            
        # Reordering to Day-Month-Year format
        logger.info(f"Parsed Date and Time (Day-Month-Year): {day}/{month}/{year} {hour}:{minute}:{second}")
	
        # Create a datetime object from the extracted and converted values
        date_obj = datetime(year, month, day, hour, minute, second)

        # Convert to ISO 8601 format
        data['converted_date_time'] = date_obj.isoformat()
        logger.info(f"ISO 8601 Date: {data['converted_date_time']}")

        #logger.info(f"Parsed Date and Time: {day}/{month}/{year} {hour}:{minute}:{second}")
        #logger.info(f"Date Object: {date_obj}")

    except ValueError as e:
        logger.error(f"Error parsing date and time: {e}")
        raise

    # Convert hex strings to float values and group by 10
    float_values = []   # Initialize an empty list to store the converted float values

    # Iterate through the hex string, starting from index 24 (after date/time)
    # and taking chunks of 8 characters (4 bytes) at a time
    for i in range(24, len(hex_string), 8):
        # Extract a chunk of 8 hex characters
        hex_chunk = hex_string[i:i+8]

        # Check if the chunk has the expected length
        if len(hex_chunk) == 8:
            # Convert the hex chunk to a float using the hex_to_float function
            float_value = hex_to_float(hex_chunk)

            # Check if the conversion was successful
            if float_value is not None:
                float_values.append(float_value)   # Append the converted float value to the list
            else:
                logger.warning(f"Invalid hex chunk: {hex_chunk}")

    # Group the float values into groups of 20 using the grp_by_twenty_per_metric function
    grouped = grp_by_twenty_per_metric(float_values)
    print(f"Grouped float values: {grouped}")

     # Verify that each group ('v0' to 'v19') contains exactly 10 values
    for i in range(20):
        if len(grouped[f'v{i}']) != 10:
            raise ValueError(f"Incorrect data format: {f'v{i}'} does not have 10 values")
    
    # Return the original data, the grouped float values, and the number of 20-value groups
    return data, grouped, len(float_values) // 20

#Formats data into InfluxDB Point objects and writes them to the database
def format_and_write_to_db(data, grouped, time_value_len):
    arr = []
    date_obj_ist = datetime.fromisoformat(data['converted_date_time'])    
    print(f"Original date_obj (IST): {date_obj_ist.isoformat()}")

    # Convert IST to UTC for InfluxDB
    date_obj_utc = date_obj_ist - timedelta(hours=5, minutes=30)

    # Calculate epoch time in milliseconds (UTC)
    epoch_d = date_obj_utc.timestamp() * 1000    #datetime object returns the Unix timestamp, which is the number of seconds since the epoch
    print(f"Epoch time in milliseconds (UTC): {epoch_d}")

    # Iterate through the 20 groups of values (v0 to v19)
    for i in range(20):
        values = grouped[f'v{i}']

        # Iterate through the values within each group, with index k
        for k, value in enumerate(values):
            next_ten_milli_sec = (epoch_d + k * 100)  # Calculate the timestamp for the current value. Each value is 100 milliseconds apart.
            timestamp = datetime.utcfromtimestamp(next_ten_milli_sec / 1000)    # Convert milliseconds back to datetime object (UTC)
            formatted_date = timestamp.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'    # Format the timestamp to the required InfluxDB string format (YYYY-MM-DDTHH:MM:SS.sssZ)
            print(f"Generated timestamp for v{i}, value {k} (UTC): {formatted_date}")

            point = (
                Point(data['pic'])   # Set the measurement name using the PIC ID
                .tag('metric', f'v{i}')   # Add a tag for the metric name (v0, v1, ..., v19)
                .field('value', value)   # Add a field for the actual value
                .time(formatted_date, write_precision=WritePrecision.NS)    # Set the timestamp with nanosecond precision
            )
            arr.append(point)
            print(f"Point added to InfluxDB batch: {point}")

    # Write the points to InfluxDB
    with InfluxDBClient(url=url, token=token, org=org) as client:
        try:
            with client.write_api(write_options=SYNCHRONOUS) as write_api:
                write_api.write(bucket=bucket, record=arr, time_precision="ns")   # Write the points to the specified bucket
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