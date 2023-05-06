import json

from flask import Flask, request, render_template
from flask_cors import CORS, cross_origin

app = Flask(__name__)
app.config['CORS_HEADERS'] = 'Content-Type'
cors = CORS(app)


@app.route('/')
@cross_origin()
def index():
    return render_template("index.html")


@app.route('/upload_gen_result', methods=['POST', 'GET'])
@cross_origin()
def store_gen_result():
    """
    format: 127.0.0.1:5000/upload_gen_result?gen_result={"user_id":"serial_num",'data_content':'data'}
    """
    gen_result = None
    response = {}

    if len(request.args):
        response["status"] = 200
        response["info"] = "generated result received"
        trans_str = request.args.to_dict().get("gen_result", None)
        gen_result = json.loads(trans_str)
        """
        we shall store our data at here, and verify
        """
    else:
        response["status"] = 500
        response["info"] = "request with empty data"
    return json.dumps(response, ensure_ascii=False)
