from flask import Flask, jsonify

app = Flask(__name__)

@app.route('/api/private', methods=['GET'])
def secret_data():
    return jsonify({
        "status": "success",
        "message": "Welcome VIP! You have accessed the protected area.",
        "secret_code": 123456
    }), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
