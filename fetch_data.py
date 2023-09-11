import os
import shutil

def gather_all_files(directory):
    all_files = set()
    for dirpath, dirnames, filenames in os.walk(directory):
        all_files.update(filenames)
    return all_files

def copy_files(src_directory, dest_directory, processed_directory):
    processed_files = gather_all_files(processed_directory)

    for filename in os.listdir(src_directory):
        # Full path of the file in the source directory
        src_filepath = os.path.join(src_directory, filename)

        # Full path of the file in the destination directory
        dest_filepath = os.path.join(dest_directory, filename)

        # Check conditions: JSON files, name starting with 9, doesn't exist in destination or processed directories
        if (filename.endswith(".json") and 
            filename.startswith("9") and 
            not os.path.exists(dest_filepath) and 
            filename not in processed_files):
            
            shutil.copy(src_filepath, dest_filepath)
            print(f"Copied {filename} to NewData")

def main():
    # Directories
    src_directory = "/home/zabbix/allcustomer/AutoQRadar-Mutaciones/10.4.0.67"
    dest_directory = "/home/cicontreras/Scripts/QR-DeviationDB/NewData"
    processed_directory = "/home/cicontreras/Scripts/QR-DeviationDB/ProcessedData"
    
    copy_files(src_directory, dest_directory, processed_directory)

if __name__ == "__main__":
    main()
