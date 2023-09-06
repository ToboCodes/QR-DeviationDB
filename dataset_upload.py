import mysql.connector
import json
import os
import shutil

# Load data and store filenames
def load_json_files(directory, processed_directory):
    data_list = []
    filenames = []
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        if filename.endswith(".json"):
            with open(filepath, 'r') as file:
                file_content = file.read()

                # Check if the file contains "503 Service Unavailable"
                if "503 Service Unavailable" in file_content:
                    print(f"File contains '503 Service Unavailable' error: {filename}. Moving to ProcessedData directory.")
                    shutil.move(filepath, os.path.join(processed_directory, filename))
                    continue

                if os.path.getsize(filepath) == 0:  # Check if the file is empty
                    print(f"Empty file: {filename}. Moving to ProcessedData directory.")
                    shutil.move(filepath, os.path.join(processed_directory, filename))
                    continue

                try:
                    data = json.loads(file_content)
                    offense_id, query_time = filename.replace(".json", "").split("-")
                    data['offense_id'] = int(offense_id)
                    data['query_time'] = int(query_time)
                    data_list.append(data)
                    filenames.append(filename)
                except json.JSONDecodeError:
                    print(f"Error decoding JSON in file: {filename}")
    return data_list, filenames


def insert_data_to_db(data_list, db_config):
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    for data in data_list:
        # Check for duplicates
        cursor.execute("""
            SELECT 1 FROM Offense WHERE offense_id = %s AND query_time = %s;
        """, (data['offense_id'], data['query_time']))
        if cursor.fetchone():
            continue

        # Set default values for empty data
        assigned_to = data.get('assigned_to', None)
        category_count = data.get('category_count', None)
        close_time = data.get('close_time', None)
        closing_reason_id = data.get('closing_reason_id', None)
        closing_user = data.get('closing_user', None)
        credibility = data.get('credibility', None)
        description = data.get('description', None)
        device_count = data.get('device_count', None)
        domain_id = data.get('domain_id', None)
        event_count = data.get('event_count', None)
        first_persisted_time = data.get('first_persisted_time', None)
        flow_count = data.get('flow_count', None)
        follow_up = data.get('follow_up', None)
        id_val = data.get('id', None)
        inactive = data.get('inactive', None)
        last_persisted_time = data.get('last_persisted_time', None)
        last_updated_time = data.get('last_updated_time', None)
        local_destination_count = data.get('local_destination_count', None)
        magnitude = data.get('magnitude', None)
        offense_source = data.get('offense_source', None)
        offense_type = data.get('offense_type', None)
        policy_category_count = data.get('policy_category_count', None)
        protected = data.get('protected', None)
        relevance = data.get('relevance', None)
        remote_destination_count = data.get('remote_destination_count', None)
        security_category_count = data.get('security_category_count', None)
        severity = data.get('severity', None)
        source_count = data.get('source_count', None)
        source_network = data.get('source_network', None)
        start_time = data.get('start_time', None)
        status = data.get('status', None)
        username_count = data.get('username_count', None)

        # INSERT INTO Offense table
        cursor.execute("""
            INSERT INTO Offense (offense_id, query_time, assigned_to, category_count, close_time, 
                                 closing_reason_id, closing_user, credibility, description, device_count, 
                                 domain_id, event_count, first_persisted_time, flow_count, follow_up, id, 
                                 inactive, last_persisted_time, last_updated_time, local_destination_count, 
                                 magnitude, offense_source, offense_type, policy_category_count, protected, 
                                 relevance, remote_destination_count, security_category_count, severity, 
                                 source_count, source_network, start_time, status, username_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (data['offense_id'], data['query_time'], assigned_to, category_count, 
              close_time, closing_reason_id, closing_user, credibility, description, device_count, 
              domain_id, event_count, first_persisted_time, flow_count, follow_up, id_val, inactive, 
              last_persisted_time, last_updated_time, local_destination_count, magnitude, offense_source, 
              offense_type, policy_category_count, protected, relevance, remote_destination_count, 
              security_category_count, severity, source_count, source_network, start_time, status, username_count))

        # INSERT INTO Categories table and OffenseCategories junction table
        categories = data.get('categories', [])
        for category in categories:
            # Check if category already exists
            cursor.execute("""
                SELECT category_id FROM Categories WHERE category_name = %s;
            """, (category,))
            result = cursor.fetchone()
            if result:
                category_id = result[0]
            else:
                cursor.execute("""
                    INSERT INTO Categories (category_name) VALUES (%s);
                """, (category,))
                category_id = cursor.lastrowid
            
            # INSERT INTO OffenseCategories
            cursor.execute("""
                INSERT INTO OffenseCategories (offense_id, query_time, category_id) 
                VALUES (%s, %s, %s);
            """, (data['offense_id'], data['query_time'], category_id))

        # INSERT INTO DestinationNetworks table
        destination_networks = data.get('destination_networks', [])
        for network in destination_networks:
            cursor.execute("""
                INSERT INTO DestinationNetworks (offense_id, query_time, network_name)
                VALUES (%s, %s, %s);
            """, (data['offense_id'], data['query_time'], network))

        # INSERT INTO LogSources table
        log_sources = data.get('log_sources', [])
        for log in log_sources:
            # Check if log source already exists
            cursor.execute("""
                SELECT 1 FROM LogSources WHERE log_source_id = %s;
            """, (log.get('id'),))
            if not cursor.fetchone():
                cursor.execute("""
                    INSERT INTO LogSources (log_source_id, name, type_id, type_name, domain_id)
                    VALUES (%s, %s, %s, %s, %s);
                """, (log.get('id'), log.get('name'), log.get('type_id'), log.get('type_name'), data.get('domain_id')))

        # INSERT INTO LocalDestinationAddress table
        local_destination_address_ids = data.get('local_destination_address_ids', [])
        for addr in local_destination_address_ids:
            cursor.execute("""
                INSERT INTO LocalDestinationAddress (offense_id, query_time, address_id)
                VALUES (%s, %s, %s);
            """, (data['offense_id'], data['query_time'], addr))

        # INSERT INTO Rules table
        rules = data.get('rules', [])
        for rule in rules:
            cursor.execute("""
                INSERT INTO Rules (offense_id, query_time, id, type)
                VALUES (%s, %s, %s, %s);
            """, (data['offense_id'], data['query_time'], rule.get('id'), rule.get('type')))

        # INSERT INTO SourceAddresses table
        source_address_ids = data.get('source_address_ids', [])
        for addr in source_address_ids:
            cursor.execute("""
                INSERT INTO SourceAddresses (offense_id, query_time, address_id)
                VALUES (%s, %s, %s);
            """, (data['offense_id'], data['query_time'], addr))

    connection.commit()
    cursor.close()
    connection.close()

def main():
    directory = "/home/cicontreras/Scripts/QR-DeviationDB/NewData"
    processed_directory = "/home/cicontreras/Scripts/QR-DeviationDB/ProcessedData"
    data_list, filenames = load_json_files(directory, processed_directory)

    db_config = {
        'user': 'root',
        'password': os.environ.get('DB_PASSWORD'),
        'host': '127.0.0.1',
        'database': 'desviaciones'
    }

    insert_data_to_db(data_list, db_config)
    
    # Move processed files to ProcessedData directory
    for filename in filenames:
        shutil.move(os.path.join(directory, filename), os.path.join(processed_directory, filename))

if __name__ == "__main__":
    main()
