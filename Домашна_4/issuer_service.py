from flask import Flask, jsonify
import requests
from bs4 import BeautifulSoup as BS

app = Flask(__name__)

BASE_URL = "https://www.mse.mk/mk/stats/symbolhistory/ADIN"

@app.route('/', methods=['GET'])
def index():
    return "Welcome to the Issuer Service API. Use /issuers to get the list of issuers."

@app.route('/issuers', methods=['GET'])
def get_issuers():
    try:
        response = requests.get(BASE_URL, headers={"User-Agent": "Mozilla/5.0"})
        if response.status_code != 200:
            return jsonify([])

        soup = BS(response.content, 'html.parser')
        option_elements = soup.select("#Code > option")

        issuers = [opt.text.strip() for opt in option_elements if not any(char.isdigit() for char in opt.text)]
        return jsonify(issuers)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(port=5001)
