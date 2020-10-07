from flask import Flask, abort, jsonify, request

from humaniki_schema.db import session_factory
from flask_sqlalchemy_session import flask_scoped_session

from humaniki_backend.query import get_properties_id, get_metrics, get_latest_fill_id
from humaniki_backend.utils import determine_population_conflict, build_gap_response, assert_gap_request_valid

app = Flask(__name__)
session = flask_scoped_session(session_factory, app)

# Note this requires updating or the process restarting after a new fill.
latest_fill_id, latest_fill_date = get_latest_fill_id(session)

@app.route("/")
def home():
    return jsonify(latest_fill_id, latest_fill_date)


@app.route("/v1/gender/gap/<string:snapshot>/<string:population>/")
def gap(snapshot, population):
    return_warnings = {}
    query_values = request.values
    try:
        valid_request = assert_gap_request_valid(snapshot, population, query_values)
    except AssertionError as ae:
        return jsonify(ae)
    #
    population_correction = determine_population_conflict(population, query_values)
    if population:
        return_warnings['population_correction'] = population_correction

    try:
        properties_id = get_properties_id(session, query_values)
    except ValueError as ve:
        raise
        return jsonify(ve)

    # TODO
    # may need to get an aggregations_id here
    aggregations_id = None

    try:
        metrics = get_metrics(session, fill_id=latest_fill_id, population=population, properties_id=properties_id,
                              aggregations_id=aggregations_id)
    except ValueError as ve:
        return jsonify(ve)
    response = build_gap_response(metrics)
    return jsonify(**response)


if __name__ == "__main__":
    app.run()
