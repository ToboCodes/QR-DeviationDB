import os
import json
import mysql.connector

def load_json_files(directory):
    data_list = []
    
    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            with open(os.path.join(directory, filename), 'r') as file:
                try:
                    data = json.load(file)
                except json.JSONDecodeError:
                    print(f"Error decoding JSON in file: {filename}")
                    continue
                
                # Extracting offense_id and query_time from the filename format: offense_id-query_time.json
                offense_id, query_time = filename.split('.')[0].split('-')
                
                data['offense_id'] = offense_id
                data['query_time'] = query_time
                
                data_list.append(data)
    return data_list



def insert_data_to_db(data_list, db_config):
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    
    for data in data_list:
        # Insert data into Offense table
        cursor.execute("""
            INSERT INTO Offense (
                offense_id, query_time, assigned_to, category_count, close_time, 
                closing_reason_id, closing_user, credibility, description, device_count, 
                domain_id, event_count, first_persisted_time, flow_count, follow_up, 
                id, inactive, last_persisted_time, last_updated_time, local_destination_count, 
                magnitude, offense_source, offense_type, policy_category_count, protected, 
                relevance, remote_destination_count, security_category_count, severity, 
                source_count, source_network, start_time, status, username_count
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            data['offense_id'], data['query_time'], data['assigned_to'], data['category_count'], 
            data['close_time'], data['closing_reason_id'], data['closing_user'], data['credibility'], 
            data['description'], data['device_count'], data['domain_id'], data['event_count'], 
            data['first_persisted_time'], data['flow_count'], data['follow_up'], data['id'], 
            data['inactive'], data['last_persisted_time'], data['last_updated_time'], 
            data['local_destination_count'], data['magnitude'], data['offense_source'], 
            data['offense_type'], data['policy_category_count'], data['protected'], data['relevance'], 
            data['remote_destination_count'], data['security_category_count'], data['severity'], 
            data['source_count'], data['source_network'], data['start_time'], data['status'], 
            data['username_count']
        ))
        
        # Insert data into other tables (e.g., Categories, DestinationNetworks, etc.)
        # This is just an example for the Categories table. Similar logic applies for other tables.
        for category in data['categories']:
            cursor.execute("""
                INSERT INTO Categories (offense_id, category_name) 
                VALUES (%s, %s);
            """, (data['offense_id'], category))
    
    connection.commit()
    cursor.close()
    connection.close()

def main():
    directory = "C:\\Desviaciones\\10.4.0.67\\"
    data_list = load_json_files(directory)
    
    db_config = {
        'user': 'root',
        'password': input("Enter MySQL password: "),
        'host': '10.4.0.159',
        'database': 'desviaciones'
    }
    
    insert_data_to_db(data_list, db_config)

if __name__ == "__main__":
    main()
