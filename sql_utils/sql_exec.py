from flask import Flask, request, jsonify
import pymysql

app = Flask(__name__)

# 创建数据库连接
db_cinder = pymysql.connect(host='172.25.61.3', user='root', password='yunyuansheng', port=3306, db='cinder_e')
db_cmp = pymysql.connect(host='172.25.61.3', user='root', password='yunyuansheng', port=3306, db='cmp_e')

# 创建游标对象
cursor_cinder = db_cinder.cursor()
cursor_cmp = db_cmp.cursor()


def query_exec(query):
    cursor_cmp.execute(query)
    db_cmp.commit()  # 提交事务
    result = cursor_cmp.fetchall()
    # db_cmp.close()
    return result