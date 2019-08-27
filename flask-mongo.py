from bson import json_util, ObjectId
from bson.json_util import dumps
import json
from datetime import datetime
from flask import Flask
from flask import jsonify
from flask import request
from flask_pymongo import PyMongo

app = Flask(__name__)

#app.config['MONGO_DBNAME'] = 'comet'
#app.config['MONGO_URI'] = 'mongodb://10.44.128.98:27017/comet'
app.config['MONGO_URI'] = 'mongodb://rabbitmq:password@10.44.94.135:27017/rabbitmq'
mongo = PyMongo(app)


@app.route("/")
def hello():
    return "Welcome to Python Flask and MongoDB! \r\n 10.44.41.74:3001/rabbitmq \r\n 10.44.41.74:3001/rabbitmq/upload"


@app.route('/rabbitmq', methods=['GET'])
def get_all_rabbit():
    try:
        data = mongo.db.message
        output = []
        for s in data.find():
            output.append(json.loads(json_util.dumps(s)))
        return jsonify(output)
    except Exception as e:
        return dumps({'error': str(e)})


@app.route('/select/<data>', methods=['GET'])
def select(data):
    try:
        data = data.strip()
        col = data.split(",")
        collection = col[0]
        key = col[1]
        value = col[2]
        print(collection)
        print(key)
        print(value)
        db = mongo.db[collection]
        print(db)
        output = []
        s = db.find({key: value})
        if s:
            for lst in s:
                output.append(json.loads(json_util.dumps(lst)))
        else:
            output = "No such name"
        return jsonify(output)
    except Exception as e:
        return dumps({'error': str(e)})


@app.route('/rabbitmq/<name>', methods=['GET'])
def get_one_name(name):
    try:
        data = mongo.db.message
        output = []
        s = data.find({'name' : name})
        if s:
            for lst in s:
                output.append(json.loads(json_util.dumps(lst)))
        else:
            output = "No such name"
        #return json.dumps(output)
        return jsonify(output)
    except Exception as e:
        return dumps({'error': str(e)})


@app.route('/insert', methods=['POST'])
def insert():
    try:
        data = request.get_json()
        if "name" in data:
            col = data.get('name')
        elif "sender_type" in data:
            col = data.get('sender_type')
        else:
            col = 'NoGroup'
        db = mongo.db[col]
        print(db)

        post_id = db.insert_one(data).inserted_id
        print(post_id)
        db.update_one({'_id': ObjectId(post_id)}, {"$set": {"timestamp": datetime.utcnow()}})
        return jsonify({'result': str(post_id)})
    except Exception as e:
        return dumps({'error': str(e)})


@app.route('/upload', methods=['POST'])
def add_star():
    try:
        data = request.get_json()
        if "name" in data:
            col = data.get('name')
        elif "sender_type" in data:
            col = data.get('sender_type')
        else:
            col = 'NoGroup'
        db = mongo.db[col]
        print(db)

        post_id = db.insert_one(data).inserted_id
        print(post_id)
        db.update_one({'_id': ObjectId(post_id)}, {"$set": {"timestamp": datetime.utcnow()}})
        return jsonify({'result': str(post_id)})
    except Exception as e:
        return dumps({'error': str(e)})


if __name__ == '__main__':
    #app.run(debug=True)
    app.run(host='0.0.0.0', port=3003)
