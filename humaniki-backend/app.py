from flask import Flask, jsonify
from humaniki_schema.schema import metric
app = Flask(__name__)

@app.route("/")
def hello():
    ametric = metric()
    return jsonify(ametric.population_id)

if __name__ == "__main__":
    app.run()
