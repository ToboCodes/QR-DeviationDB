import mysql.connector
import pandas as pd
import sys
import datetime
import os
from openpyxl.styles import Alignment, Border, Side

def fetch_data(start_date):
    db_config = {
        'host': 'localhost',
        'user': 'root',
        'password': os.environ.get('DB_PASSWORD'),
        'database': 'desviaciones'
    }

    # Convert date to UNIX time in ms
    unix_timestamp = int(datetime.datetime.strptime(start_date, '%Y-%m-%d').timestamp() * 1000)

    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor(dictionary=True)
    
    # JOIN with domain table
    cursor.execute("""
        SELECT o.offense_id, d.name AS domain_name, o.category_count, o.device_count, o.event_count, 
           o.local_destination_count, o.magnitude, o.username_count, o.query_time, o.source_count,
           o.remote_destination_count, o.security_category_count
        FROM Offense o
        JOIN Domains d ON o.domain_id = d.domain_id
        WHERE o.offense_id IN (
            SELECT DISTINCT offense_id
            FROM Offense
            WHERE last_updated_time >= %s
        )
        ORDER BY o.offense_id, o.query_time;""", (unix_timestamp,))

    return cursor.fetchall()

def analyze_data(data):
    results = []
    for i in range(0, len(data) - 4, 5):
        for j in range(0, 4):  # This loop goes from 0 to 3, inclusive
            current_data = data[i + j]
            next_data = data[i + j + 1]

            if current_data["offense_id"] != next_data["offense_id"]:
                break

            for column in ["category_count", "device_count", "local_destination_count", "magnitude", "username_count", "source_count", "remote_destination_count", "security_category_count", "event_count"]:
                delta_value = next_data[column] - current_data[column]
                if delta_value > 0:
                    results.append({
                        'offense_id': current_data['offense_id'],
                        'domain_name': current_data['domain_name'],
                        'column': column,
                        'delta_value': delta_value,
                        'changed_value': next_data[column],
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
            summary[key]['magnitude'] += row['delta_value']*2
        elif row['column'] == 'device_count':
            if row['delta_value'] > 2:
                summary[key]['magnitude'] += 5
        elif row['column'] == 'event_count':
            increase = (row['delta_value'] >= 50) and row['query_time'] != 4
            if increase:
                summary[key]['magnitude'] += 3
        elif row['column'] == 'local_destination_count':
            summary[key]['magnitude'] += 3
        elif row['column'] == 'magnitude':
            if row['changed_value'] > 4 and row['changed_value'] < 7:
                summary[key]['magnitude'] += 3
            elif row['changed_value'] == 7:
                summary[key]['magnitude'] += 6
            elif row['changed_value'] > 7:
                summary[key]['magnitude'] += 10
            else:
                summary[key]['magnitude'] += 2
        elif row['column'] == 'username_count':
            summary[key]['magnitude'] += 1
        elif row['column'] == 'source_count':
            summary[key]['magnitude'] += 3
        elif row['column'] == 'remote_destination_count':
            summary[key]['magnitude'] += 2
        elif row['column'] == 'security_category_count':
            summary[key]['magnitude'] += 1


    # List of dictionaries for easier document writing
    summary_list = [{'domain_name': key[0], 'offense_id': key[1], 'evolution': len(val['evolution']), 'magnitude': val['magnitude']} for key, val in summary.items()]
    return sorted(summary_list, key=lambda x: x['domain_name'])



def auto_adjust_columns_width(sheet):
    for column in sheet.columns:
        max_length = 0
        column = [cell for cell in column]
        for cell in column:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(cell.value)
            except:
                pass
        adjusted_width = (max_length + 2)
        sheet.column_dimensions[column[0].column_letter].width = adjusted_width

def write_to_excel(results, summary, filename):
    results_df = pd.DataFrame(results)
    summary_df = pd.DataFrame(summary)
    
    # Omit the 'changed_value' column
    results_df = results_df[['offense_id', 'domain_name', 'column', 'delta_value', 'query_time']]
    
    # Set column names for the 'Data' sheet
    results_df.rename(columns={
        'offense_id': 'Ofensa',
        'domain_name': 'Cliente',
        'column': 'Atributo',
        'delta_value': 'Delta',
        'query_time': 'Muestra'
    }, inplace=True)

    # Set column names for the 'Resumen' sheet
    summary_df.rename(columns={
        'domain_name': 'Cliente',
        'offense_id': 'Ofensa',
        'evolution': 'Evolución',
        'magnitude': 'Magnitud'
    }, inplace=True)

    with pd.ExcelWriter(f'Reporte Mutaciones {filename}.xlsx', engine='openpyxl') as writer:
        # Set filters and data sorting
        # summary_df = summary_df[summary_df['Evolución'] != 1]  # Filter out rows where 'Evolución' = 1
        summary_df.sort_values(by='Magnitud', ascending=False, inplace=True)  # Sort by 'Magnitud'
        summary_df.to_excel(writer, sheet_name='Resumen', index=False)
        results_df.to_excel(writer, sheet_name='Data', index=False)
        
        workbook  = writer.book
        results_sheet = workbook['Data']
        summary_sheet = workbook['Resumen']
        
        # Hide the 'Cliente' column in the 'Data' sheet
        results_sheet.column_dimensions['B'].hidden = True

        # Set cell borders
        border = Border(left=Side(style='thin'), 
                        right=Side(style='thin'), 
                        top=Side(style='thin'), 
                        bottom=Side(style='thin'))

        # Apply styles and filters
        for sheet in [results_sheet, summary_sheet]:
            last_col_letter = sheet.cell(row=1, column=sheet.max_column).column_letter
            sheet.auto_filter.ref = f"A1:{last_col_letter}1"
            
            for row in sheet.iter_rows():
                for cell in row:
                    cell.border = border
                    cell.alignment = Alignment(horizontal='left')
            auto_adjust_columns_width(sheet)

def main():
    if len(sys.argv) != 2:
        print("Usage: script.py YYYY-MM-DD")
        sys.exit(1)

    start_date = sys.argv[1]
    data = fetch_data(start_date)
    results = analyze_data(data)
    summary = generate_summary(results)
    
    # Write Excel with two sheets
    write_to_excel(results, summary, start_date)

if __name__ == '__main__':
    main()
