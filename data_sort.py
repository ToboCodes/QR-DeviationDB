import os
import json
from datetime import datetime

def move_files(base_path="."):
    if not os.path.exists(base_path):
        print(f"The path {base_path} does not exist.")
        return

    files_in_base_path = [f for f in os.listdir(base_path) if os.path.isfile(os.path.join(base_path, f))]
    
    for prefix in range(5):  # Looping from 0 to 4 inclusive
        for file in sorted(files_in_base_path):  # Sorting to ensure we start with -0.json
            if file.endswith(f"-{prefix}.json"):
                # Initial check with -0.json
                filepath = os.path.join(base_path, file)
                first_persisted_time = get_date_from_file(filepath)

                # If date not found in -0.json, look in -1.json and so on
                if not first_persisted_time:
                    for i in range(1, 5):
                        temp_filepath = filepath.replace(f"-{prefix}.json", f"-{i}.json")
                        if os.path.exists(temp_filepath):
                            first_persisted_time = get_date_from_file(temp_filepath)
                        if first_persisted_time:
                            break

                if first_persisted_time:
                    # Convert UNIX time to YYYY-MM
                    folder_name = datetime.utcfromtimestamp(first_persisted_time / 1000).strftime('%Y-%m')
                    destination_folder = os.path.join(base_path, folder_name)
                    if not os.path.exists(destination_folder):
                        os.makedirs(destination_folder)

                    # Move the file
                    os.rename(filepath, os.path.join(destination_folder, file))
                else:
                    # If no valid date found, move to Errors folder
                    error_folder = os.path.join(base_path, 'Errors')
                    if not os.path.exists(error_folder):
                        os.makedirs(error_folder)
                    os.rename(filepath, os.path.join(error_folder, file))

def get_date_from_file(filepath):
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
            return data.get('first_persisted_time', None)
    except Exception as e:
        print(f"Error reading file {filepath}: {str(e)}")
        return None

move_files("/home/cicontreras/Scripts/QR-DeviationDB/ProcessedData")
