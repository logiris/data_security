from flask import Flask, render_template, request, jsonify
import random
from datetime import datetime, timedelta
import json
import os

app = Flask(__name__)

# 生成模拟数据
def generate_mock_data():
    users = []
    for i in range(1, 101):
        user = {
            'id': f'user_{i:03d}',
            'username': f'用户{i}',
            'phone': f'1{random.randint(3,9)}{random.randint(100000000, 999999999)}',
            'comment': f'这是用户{i}的评论，商品质量很好，服务态度也不错。',
            'rating': random.randint(3, 5),
            'date': (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d %H:%M:%S')
        }
        users.append(user)
    return users

# 保存模拟数据
mock_data = generate_mock_data()
with open('mock_data.json', 'w', encoding='utf-8') as f:
    json.dump(mock_data, f, ensure_ascii=False, indent=2)

@app.route('/')
def index():
    page = int(request.args.get('page', 1))
    per_page = 10
    total_pages = (len(mock_data) + per_page - 1) // per_page
    
    start = (page - 1) * per_page
    end = start + per_page
    current_page_data = mock_data[start:end]
    
    return render_template('index.html',
                         users=current_page_data,
                         current_page=page,
                         total_pages=total_pages)

@app.route('/api/comments')
def api_comments():
    page = int(request.args.get('page', 1))
    per_page = 10
    total_pages = (len(mock_data) + per_page - 1) // per_page
    
    start = (page - 1) * per_page
    end = start + per_page
    current_page_data = mock_data[start:end]
    
    return jsonify({
        'data': current_page_data,
        'current_page': page,
        'total_pages': total_pages
    })

if __name__ == '__main__':
    app.run(debug=True) 