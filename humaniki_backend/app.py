from flask import Flask, abort, jsonify, request

from humaniki_schema.db import session_factory
from flask_sqlalchemy_session import flask_scoped_session

from humaniki_backend.query import get_properties_id, get_aggregations_ids, get_metrics, get_latest_fill_id, \
    order_query_params
from humaniki_backend.utils import determine_population_conflict, build_gap_response, assert_gap_request_valid
from humaniki_schema.utils import Properties

app = Flask(__name__)
session = flask_scoped_session(session_factory, app)

# Note this requires updating or the process restarting after a new fill.
latest_fill_id, latest_fill_date = get_latest_fill_id(session)


@app.route("/")
def home():
    return jsonify(latest_fill_id, latest_fill_date)


@app.route("/v1/<string:bias>/gap/<string:snapshot>/<string:population>/properties")
def gap(bias, snapshot, population):
    return_warnings = {}
    errors = {}
    query_params = request.values
    # TODO catch these errors in their constituent parts and then have the logic handled up here
    try:
        #TODO include validating bias
        valid_request = assert_gap_request_valid(snapshot, population, query_params)
    except AssertionError as ae:
        return jsonify(ae)
    # handle populations
    population_id, corrected = determine_population_conflict(population, query_params)
    if corrected:
        return_warnings['population_corrected to'] = population_id
    # order query params by property pid
    ordered_query_params = order_query_params(query_params)
    # get properties-id
    try:
        bias_property = getattr(Properties, bias.upper()).value
        properties_id = get_properties_id(session, ordered_query_params, bias_property=bias_property)
    except ValueError as ve:
        errors['properties_id'] = str(ve)
    # get aggregations-id
    try:
        aggregations_id = get_aggregations_ids(session, ordered_query_params)
    except ValueError as ve:
        errors['aggregations_id'] = str(ve)
    # get metric
    try:
        metrics = get_metrics(session, fill_id=latest_fill_id, population_id=population_id, properties_id=properties_id,
                              aggregations_id=aggregations_id)
    except ValueError as ve:
        errors['aggregations_id'] = str(ve)
    # convert table rows to jsonable dict
    response = build_gap_response(metrics)
    return jsonify(**response)


if __name__ == "__main__":
    app.run()
