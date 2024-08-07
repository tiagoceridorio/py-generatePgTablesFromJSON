import os
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def count_records_in_json(file_path):
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
        return len(data)
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        return 0

def count_records_in_directory(directory_path):
    record_counts = {}
    try:
        for filename in os.listdir(directory_path):
            if filename.endswith('.json'):
                file_path = os.path.join(directory_path, filename)
                record_count = count_records_in_json(file_path)
                record_counts[filename] = record_count
                logger.info(f"File {filename} contains {record_count} records.")
    except Exception as e:
        logger.error(f"Error processing directory {directory_path}: {e}")
    return record_counts

def main():
    directory_path = "./"
    record_counts = count_records_in_directory(directory_path)

    total_records = sum(record_counts.values())
    for filename, count in record_counts.items():
        print(f"{filename}: {count} records")
    print(f"Total records in all JSON files: {total_records}")

if __name__ == "__main__":
    main()
