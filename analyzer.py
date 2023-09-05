import mysql.connector
import csv
import sys
import datetime

def fetch_data(start_date):
    db_config = {
        'host': 'localhost',
        'user': 'root',
        'password': input("Enter MySQL password: "),
        'database': 'desviaciones'
    }

    # Convert the start date to UNIX timestamp in milliseconds
    unix_timestamp = int(datetime.datetime.strptime(start_date, '%Y-%m-%d').timestamp() * 1000)

    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor(dictionary=True)
    
    # Joining with the Domains table to get domain names
    cursor.execute("""
        SELECT o.offense_id, d.name AS domain_name, o.category_count, o.device_count, o.event_count, 
               o.local_destination_count, o.magnitude, o.username_count, o.query_time
        FROM Offense o
        JOIN Domains d ON o.domain_id = d.domain_id
        WHERE o.last_updated_time >= %s
        ORDER BY o.offense_id, o.query_time;
    """, (unix_timestamp,))

    return cursor.fetchall()

def analyze_data(data):
    results = []
    for i in range(0, len(data) - 4, 5):
        for j in range(0, 4):  # This loop goes from 0 to 3, inclusive
            current_data = data[i + j]
            next_data = data[i + j + 1]
            
            if current_data["offense_id"] != next_data["offense_id"]:
                break

            for column in ["category_count", "device_count", "local_destination_count", "magnitude", "username_count"]:
                if current_data[column] < next_data[column]:
                    results.append({
                        'offense_id': current_data['offense_id'],
                        'domain_name': current_data['domain_name'],
                        'column': column,
                        'initial_value': current_data[column],
                        'changed_value': next_data[column],
                        'query_time': next_data['query_time']
                    })

            event_count_delta = next_data['event_count'] - current_data['event_count']
            if event_count_delta >= 50 and (current_data['event_count'] == 0 or event_count_delta / current_data['event_count'] >= 2):
                results.append({
                    'offense_id': current_data['offense_id'],
                    'domain_name': current_data['domain_name'],
                    'column': 'event_count',
                    'initial_value': current_data['event_count'],
                    'changed_value': next_data['event_count'],
                    'query_time': next_data['query_time']
                })
    return results

def generate_summary(results):
    summary = {}
    for row in results:
        key = (row['domain_name'], row['offense_id'])
        if key not in summary:
            summary[key] = {'evolution': set(), 'magnitude': 0}
        
        summary[key]['evolution'].add(row['query_time'])

        if row['column'] == 'category_count':
            summary[key]['magnitude'] += 5
        elif row['column'] == 'device_count':
            summary[key]['magnitude'] += 10
        elif row['column'] == 'event_count':
            summary[key]['magnitude'] += 2
        elif row['column'] == 'local_destination_count':
            summary[key]['magnitude'] += 4
        elif row['column'] == 'magnitude':
            summary[key]['magnitude'] += 8
        elif row['column'] == 'username_count':
            summary[key]['magnitude'] += 2

    # Convert the summary dictionary to a list of dictionaries for easier CSV writing
    summary_list = [{'domain_name': key[0], 'offense_id': key[1], 'evolution': len(val['evolution']), 'magnitude': val['magnitude']} for key, val in summary.items()]
    return sorted(summary_list, key=lambda x: x['domain_name'])


def write_to_csv(data, filename, fieldnames):
    with open(filename, 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

def main():
    if len(sys.argv) != 2:
        print("Usage: script.py YYYY-MM-DD")
        sys.exit(1)

    start_date = sys.argv[1]
    data = fetch_data(start_date)
    results = analyze_data(data)
    summary = generate_summary(results)
    
    # Write both CSV files
    write_to_csv(results, f'Reporte Mutaciones {start_date}.csv', ['offense_id', 'domain_name', 'column', 'initial_value', 'changed_value', 'query_time'])
    write_to_csv(summary, f'Reporte Resumen {start_date}.csv', ['domain_name', 'offense_id', 'evolution', 'magnitude'])


if __name__ == '__main__':
    main()
