from flask import Flask, jsonify, request
import requests
from bs4 import BeautifulSoup as BS

app = Flask(__name__)

BASE_URL = "https://www.mse.mk/mk/stats/symbolhistory/{issuer_code}"

@app.route('/annual_data', methods=['POST'])
def get_annual_data():
    data = request.get_json()
    issuer_code = data.get('issuer_code')
    year = data.get('year')

    if not issuer_code or not year:
        return jsonify({"error": "Missing parameters"}), 400

    try:
        payload = {
            'Code': issuer_code,
            'FromDate': f"01.01.{year}",
            'ToDate': f"31.12.{year}"
        }
        response = requests.post(BASE_URL.format(issuer_code=issuer_code), data=payload)

        if response.status_code != 200:
            return jsonify([])

        soup = BS(response.content, 'html.parser')
        rows = soup.select("#resultsTable > tbody > tr")
        data = [{"Date": row.select("td")[0].text.strip(), "Price": row.select("td")[1].text.strip()} for row in rows]

        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(port=5002)
