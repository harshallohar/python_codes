# Hexadecimal Data Processor with InfluxDB Integration

This project is a Python-based AWS Lambda function that processes hexadecimal data from an input JSON, converts it into human-readable date-time and grouped float values, and writes the processed data to an InfluxDB database. The function is designed for real-time data processing and efficient storage of grouped metrics.

## Features

- **Hexadecimal Data Parsing**: Converts a hexadecimal string into a date-time object and float values.
- **Data Grouping**: Groups 200 float values into 20 metrics, each containing 10 values.
- **InfluxDB Integration**: Stores the processed data in an InfluxDB database with precise timestamps.
- **Error Handling**: Robust error handling for invalid input and data conversion issues.
- **Timezone Support**: Handles time conversions between IST (Indian Standard Time) and UTC for InfluxDB compatibility.

## Requirements

- Python 3.7 or later
- AWS Lambda environment
- An InfluxDB instance

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/<your-username>/<repository-name>.git
   cd <repository-name>
