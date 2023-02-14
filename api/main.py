#main.py
from flask import Flask, jsonify, request
import datetime, os
import proto_message_pb2 as bqm
import bigquery_manager

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
    if request.method == 'POST':
        if not request.is_json:
            return jsonify({"msg": "Missing JSON in request"}), 400  


if __name__ == '__main__':
  app.run(debug=True)
