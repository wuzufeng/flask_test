import sqlalchemy
import redis
from flask import Flask, jsonify
from flask import request
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow

app = Flask(__name__)
# 载入DB配置
app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://emqx_admin:cloudpublic@127.0.0.1:5432/emqx_cloud"
# 载入redis配置
r = redis.StrictRedis(host="127.0.0.1", port=6379, db=0, password="cloudpublic")
# 创建一个数据库引擎
db = SQLAlchemy(app)
# 初始化marshmallow
ma = Marshmallow(app)

# ORM
class User(db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(20),unique=True)
    email = db.Column(db.String(100))

    def __init__(self, username, email):
        self.username = username
        self.email = email

class UserSchema(ma.Schema):
    class Meta:
        fields = ('id', 'username', 'email')

User_schema = UserSchema()
Users_schema = UserSchema(many=True)

# 创建用户
@app.route('/add_user', methods=['POST'])
def add_user():
    username = request.json['username']
    email = request.json['email']
    new_user = User(username, email)
    try:
        # 数据库添加用户
        db.session.add(new_user)
        db.session.commit()
    except sqlalchemy.exc.IntegrityError:
        return {'msg':'用户名重复'}
    else:
        # 用户信息存入redis
        r.set(name=new_user.id, value=str(User_schema.dump(new_user)))
        return User_schema.jsonify(new_user)

# 获得所有用户
@app.route('/all_users', methods=['GET'])
def get_all_users():
    all_users = User.query.all()
    result = Users_schema.dump(all_users)
    return jsonify(result)

# 删除用户
@app.route('/delete_user/<id>', methods=['DELETE'])
def delete_user(id):
    try:
        user = User.query.get(id)
        db.session.delete(user)
        db.session.commit()
    except sqlalchemy.orm.exc.UnmappedInstanceError:
        return {'msg':'该用户不存在'}
    else:
        # redis删除该用户
        r.delete(id)
        return User_schema.jsonify(user)

# 查找用户
@app.route('/get_user/<id>', methods=['GET'])
def get_user(id):
    # 查询redis获得用户信息
    user = r.get(id)
    if(user == None):
        # redis中不存在，在DB中查找
        user = User.query.get(id)
        if(user == None):
            return {'msg': '该用户不存在'}
        else:
            return User_schema.jsonify(user)
    else:
        str_user = user.decode('UTF-8')
        data = eval(str_user)
        return User_schema.jsonify(data)

if __name__ == '__main__':
    app.run()


