from bson import json_util, ObjectId
import json
from flask import Flask
from flask import jsonify
from flask import request
from flask_pymongo import PyMongo

app = Flask(__name__)

app.config['MONGO_DBNAME'] = 'rabbitmq'
#app.config['MONGO_URI'] = 'mongodb://10.44.128.98:27017/comet'
app.config['MONGO_URI'] = 'mongodb://rabbitmq:password@10.44.94.135:27017/rabbitmq'
mongo = PyMongo(app)

@app.route('/rabbitmq', methods=['GET'])
def get_all_rabbit():
    data = mongo.db.message
    output = []
    for s in data.find():
        output.append(json.loads(json_util.dumps(s)))
    return jsonify(output)


@app.route('/rabbitmq/<string:name>', methods=['GET'])
def get_one_star(name):
    data = mongo.db.message
    s = data.find({'name' : name})
    print(type(s))
    print(s)
    if s:
        #output = json.loads(json_util.dumps(s))
        output = []
        for lst in s:
            output.append(json.loads(json_util.dumps(lst)))
    else:
        output = "No such name"
    print(type(output))
    print(output)
    return jsonify(output)


@app.route('/upload', methods=['POST'])
def add_star():
    post = request.get_json()
    print (post)
    posts = mongo.db.message
    post_id = posts.insert_one(post).inserted_id
    print(post_id)
    return jsonify({'result' : str(post_id)})


if __name__ == '__main__':
    #app.run(host='0.0.0.0', port=5001, debug=True)
    app.run(host='0.0.0.0', port=3003)