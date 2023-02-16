#main.py
from flask import Flask, jsonify, request
import datetime, os
import proto_message_pb2 as bqm
import bigquery_manager
import request_parser
import logging
import table_backups

app = Flask(__name__)

@app.route('/', methods=['GET'])
def home():
    return '''<h1>Globant Data Engineer Test</h1>
<p>A API developed to fill an HR analytics database.</p>'''

@app.errorhandler(404)
def page_not_found(e):
    return "<h1>404</h1><p>The resource could not be found.</p>", 404


@app.route('/api/v1/add_records/jobs', methods=['POST'])
def jobs():
    table_name = 'jobs'
    if request.method == 'POST':
        if not request.is_json:
            return jsonify({"msg": "Missing JSON in request"}), 400  
        return aux_petition_handler(request, bqm.Job, table_name)

@app.route('/api/v1/add_records/departments', methods=['POST'])
def departments():
    table_name = 'departments'
    if request.method == 'POST':
        if not request.is_json:
            return jsonify({"msg": "Missing JSON in request"}), 400  
        return aux_petition_handler(request, bqm.Job, table_name)

@app.route('/api/v1/add_records/hired_employees', methods=['POST'])
def hired_employees():
    table_name = 'hired_employees'
    if request.method == 'POST':
        if not request.is_json:
            return jsonify({"msg": "Missing JSON in request"}), 400  
        if len(request.get_json()) > 1000:
            return jsonify({"msg": "Limit of 1k records per request!"}), 400 
            
        return aux_petition_handler(request, bqm.Hired_employees, table_name)

def aux_petition_handler(request, proto_class, table_name):
    try:
        good_rec, bad_rec = request_parser.send_parsed_messages_to_bq(request, proto_class=proto_class, table_name=table_name)
        return ({"msg": f"{good_rec} Records sent to BigQuery table {table_name} and {bad_rec} sent to error logs table."}), 200
    except Exception as e:
        error_message = str(e)
        return ({"msg": f"Error found {error_message}"}), 400

def aux_backup(table_name):
    try:
        msg = table_backups.create_table_backup(table_name)
        return ({"msg": msg}), 200
    except Exception as e:
        error_message = str(e)
        return ({"msg": f"Error found {error_message}"}), 400

def aux_restore(table_name):
    try:
        msg = table_backups.restore_table(table_name)
        return ({"msg": msg}), 200
    except Exception as e:
        error_message = str(e)
        return ({"msg": f"Error found {error_message}"}), 400

@app.route('/api/v1/backup/jobs', methods=['GET'])
def backup_jobs():
    table_name = 'jobs'
    return aux_backup(table_name)

@app.route('/api/v1/backup/departments', methods=['GET'])
def backup_departments():
    table_name = 'departments'
    return aux_backup(table_name)

@app.route('/api/v1/backup/hired_employees', methods=['GET'])
def backup_hired_employees():
    table_name = 'hired_employees'
    return aux_backup(table_name)

@app.route('/api/v1/restore/jobs', methods=['GET'])
def restore_jobs():
    table_name = 'jobs'
    return aux_restore(table_name)

@app.route('/api/v1/restore/departments', methods=['GET'])
def restore_departments():
    table_name = 'departments'
    return aux_restore(table_name)

@app.route('/api/v1/restore/hired_employees', methods=['GET'])
def restore_hired_employees():
    table_name = 'hired_employees'
    return aux_restore(table_name)

@app.route('/api/v1/test', methods=['POST'])
def test_json():
    #table_name = 'hired_employees's
    if request.method == 'POST':
        if not request.is_json:
            return jsonify({"msg": "Missing JSON in request"}), 400  
        return jsonify({"msg": str((request.get_json()[0]))}), 200

if __name__ == '__main__':
  app.run(debug=True, host='0.0.0.0')
