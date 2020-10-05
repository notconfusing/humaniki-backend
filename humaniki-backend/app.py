from flask import Flask, abort, jsonify
from humaniki_schema.schema import metric, metric_aggregations_j, metric_aggregations_n, metric_coverage, metric_properties_j, metric_properties_n, project, human_property, human, human_country, human_occupation, human_sitelink
from humaniki_schema.utils import PopulationDefinition, FillType

from humaniki_schema.db import session_factory
from flask_sqlalchemy_session import flask_scoped_session

app = Flask(__name__)
session = flask_scoped_session(session_factory, app)

@app.route("/metric/<int:fill_id>")
def metric_example(fill_id):
    ametric = session.query(metric).filter(metric.fill_id==fill_id).first()
    if ametric is None:
        abort(404)
    return jsonify(**ametric.to_dict())

if __name__ == "__main__":
    app.run()

