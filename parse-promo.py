import json
import re
import argparse
import psycopg2

def extract_fields_from_log(log_entry):
    try:
        # Regular expression to extract TXN ID
        txn_id_match = re.search(r'TXN ID: ([\w-]+)', log_entry)
        txn_id = txn_id_match.group(1) if txn_id_match else None

        # Extract JSON part of the log
        json_part_match = re.search(r'AnsIptspPstnRequestDto: (.*)', log_entry)
        json_part = json_part_match.group(1) if json_part_match else None

        if json_part:
            # Parse the JSON part
            data = json.loads(json_part)

            # Extract required fields
            msisdn = data.get('msisdn').split(',')
            message = data.get('message')
            rn_code = data.get('rn_code')
            is_unicode = data.get('isUnicode')
            client_trans_id = data.get('clienttransid')

            # Calculate message count
            message_count = calculate_message_count(message, is_unicode)
            msg_len = len(message)
            return {
                "TXN ID": txn_id,
                "msisdn": msisdn,
                "msisdn_count": len(msisdn),
                "message": message,
                "rn_code": rn_code,
                "isUnicode": is_unicode,
                "client_trans_id": client_trans_id,
                "message_count": message_count,
                "msg_len": msg_len
            }
    except Exception as e:
        print(f"Error extracting fields from log entry: {e}")
    return None

def calculate_message_count(message, is_unicode):
    if is_unicode:
        if len(message) <= 70:
            return 1
        else:
            return (len(message) + 66) // 67  # Multipart Unicode message length calculation
    else:
        if len(message) <= 160:
            return 1
        else:
            return (len(message) + 155) // 156  # Multipart ASCII message length calculation

def read_log_file(file_path, db_params):
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                result = extract_fields_from_log(line)
                if result:
                    try:
                        cursor.execute('''
                        INSERT INTO sms_count_promo_202404  (txn_id, msisdn, msisdn_count, message, rn_code, is_unicode, message_count, client_trans_id, msg_len)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (txn_id,client_trans_id) DO NOTHING
                        ''', (result['TXN ID'], result['msisdn'], result['msisdn_count'], result['message'], result['rn_code'], result['isUnicode'], result['message_count'], result['client_trans_id'], result['msg_len']))
                        conn.commit()
                    except Exception as e:
                        print(f"Error inserting data into database: {e}")
                        conn.rollback()
    except Exception as e:
        print(f"Error reading log file or connecting to database: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def main():
    parser = argparse.ArgumentParser(description="Extract fields from log file and write to PostgreSQL database.")
    parser.add_argument('log_file_path', type=str, help="Path to the log file")
    parser.add_argument('--dbname', type=str, default='a2p_reporting', help="Database name (default: a2p_reporting)")
    parser.add_argument('--user', type=str, default='postgres', help="Database user")
    parser.add_argument('--password', type=str, default='postgres', help="Database password")
    parser.add_argument('--host', type=str, default='localhost', help="Database host (default: localhost)")
    parser.add_argument('--port', type=str, default='5432', help="Database port (default: 5432)")
    args = parser.parse_args()

    db_params = {
            'dbname': args.dbname,
        'user': args.user,
        'password': args.password,
        'host': args.host,
        'port': args.port
    }

    read_log_file(args.log_file_path, db_params)

if __name__ == "__main__":
    main()
