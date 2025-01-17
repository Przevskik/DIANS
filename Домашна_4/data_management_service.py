from flask import Flask, request, jsonify
import os
import pandas as pd

app = Flask(__name__)

DATA_FOLDER = "data"

def save_data(issuer, data):
    file_path = os.path.join(DATA_FOLDER, f"{issuer}.csv")
    df = pd.DataFrame(data)
    if os.path.exists(file_path):
        df_existing = pd.read_csv(file_path)
        df = pd.concat([df_existing, df], ignore_index=True)
    df.to_csv(file_path, index=False, encoding="utf-8-sig")

@app.route('/save_data', methods=['POST'])
def save_data_endpoint():
    data = request.get_json()
    issuer = data.get('issuer')
    data_values = data.get('data')

    if not issuer or not data_values:
        return jsonify({"error": "Missing parameters"}), 400

    try:
        save_data(issuer, data_values)
        return jsonify({"message": f"Data for {issuer} saved successfully!"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(port=5003)
