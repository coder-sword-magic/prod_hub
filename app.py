from atch import open_handler, mod_handler, divid_handler, doc_handler
from caldr import auto_review
from mail_tools import datahub
from box import box_handler
from flask import Flask, jsonify, request
from tools.db_tools import DBT
import os
os.environ['ENV'] = 'dev'


app = Flask(__name__)


@app.route('/')
def home():
    mode = os.environ.get('ENV')
    name = request.args.get('name','guest')
    return f'hello, we are in {mode} {name}\n'

@app.route('/prod/email')
def collect_email():
    # time.sleep(5)
    datahub.main()
    return 'ok'

@app.route('/prod/box/<req_id>')
def box(req_id):
    # time.sleep(5)
    box_handler.main(req_id)
    return req_id


@app.route('/prod/all_products')
def fetch_all_products():
    # time.sleep(5)
    dbt = DBT()
    raw = dbt.load_from_db(f'''select * from {dbt.product_tb}''')
    print(raw)
    return jsonify(raw.T.to_dict())


@app.route('/prod/all_boxes')
def fetch_all_boxes():
    # time.sleep(5)
    dbt = DBT()
    raw = dbt.load_from_db(f'''select * from {dbt.box_tb}''')
    print(raw)
    return jsonify(raw[['file_name', 'id', 'box_name']].T.to_dict())


@app.route('/prod/all_attachments')
def fetch_all_attachments():
    # time.sleep(5)
    dbt = DBT()
    raw = dbt.load_from_db(f'''select * from {dbt.atch_tb}''')
    print(raw)
    return jsonify(raw.T.to_dict())


@app.route('/prod/all_calendar')
def fetch_all_calendar():
    # time.sleep(5)
    dbt = DBT()
    raw = dbt.load_from_db(f'''select * from {dbt.calendar_tb}''')
    print(raw)
    return jsonify(raw.T.to_dict())


@app.route('/prod/doc/<req_id>')
def handle_doc(req_id):
    # time.sleep(5)
    resp = doc_handler.main(req_id)
    return jsonify(resp)


@app.route('/prod/open/<req_id>')
def handle_open(req_id):
    # time.sleep(5)
    resp = open_handler.main(req_id)
    return jsonify(resp)


@app.route('/prod/mod/<req_id>')
def handle_mod(req_id):
    # time.sleep(5)
    resp = mod_handler.main(req_id)
    return jsonify(resp)


@app.route('/prod/divid/<req_id>')
def handle_divid(req_id):
    # time.sleep(5)
    resp = divid_handler.main(req_id)
    return jsonify(resp)


@app.route('/prod/auto_review')
def handle_auto_review():
    # time.sleep(5)
    auto_review.main()
    return 'ok\n'