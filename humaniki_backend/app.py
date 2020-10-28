from flask import Flask, abort, jsonify, request

from humaniki_schema.db import session_factory
from flask_cors import CORS

from flask_sqlalchemy_session import flask_scoped_session

from humaniki_backend.query import get_aggregations_ids, get_metrics, build_gap_response, build_metrics, get_metrics_count
from humaniki_backend.utils import determine_population_conflict, assert_gap_request_valid, \
    order_query_params, get_pid_from_str, determine_fill_id, is_property_exclusively_citizenship
from humaniki_schema.queries import get_properties_obj, get_latest_fill_id
from humaniki_schema.utils import Properties, make_fill_dt

app = Flask(__name__)
CORS(app)
session = flask_scoped_session(session_factory, app)

# Note this requires updating or the process restarting after a new fill.
latest_fill_id, latest_fill_date = get_latest_fill_id(session)
app.latest_fill_id = latest_fill_id


@app.route("/")
def home():
    return jsonify(latest_fill_id, latest_fill_date)


@app.route("/v1/<string:bias>/gap/<string:snapshot>/<string:population>/properties")
def gap(bias, snapshot, population):
    latest_fill_id, latest_fill_date = get_latest_fill_id(session)
    return_warnings = {}
    errors = {}
    query_params = request.values
    # TODO catch these errors in their constituent parts and then have the logic handled up here
    try:
        # TODO include validating bias
        valid_request = assert_gap_request_valid(snapshot, population, query_params)
    except AssertionError as ae:
        return jsonify(ae)
    # handle snapshot
    requested_fill_id, requested_fill_date, snapshot_corrected = determine_fill_id(session, snapshot, latest_fill_id,
                                                                                   latest_fill_date)
    # print(f"Fills {requested_fill_id} {requested_fill_date}")
    if snapshot_corrected:
        return_warnings['snapshot_corrected to'] = requested_fill_id
    # handle populations
    population_id, population_name, population_corrected = determine_population_conflict(population, query_params)
    if population_corrected:
        return_warnings['population_corrected to'] = population_name
    # order query params by property pid
    ordered_query_params, non_orderable_query_params = order_query_params(query_params)
    # get properties-id
    try:
        bias_property = get_pid_from_str(bias)
        ordered_properties = ordered_query_params.keys()
        properties_id = get_properties_obj(session=session, dimension_properties=ordered_properties,
                                           bias_property=bias_property)
        # properties_id = get_properties_id(session, ordered_properties, bias_property=bias_property)
    except ValueError as ve:
        errors['properties_id'] = str(ve)
    # get aggregations-id
    try:
        aggregations_id = get_aggregations_ids(session, ordered_query_params)
    except ValueError as ve:
        errors['aggregations_id'] = str(ve)
    # get metric
    try:
        # default the label lang to 'en' if not set
        label_lang = non_orderable_query_params['label_lang'] if 'label_lang' in non_orderable_query_params else None
        metrics, represented_biases = build_metrics(session, fill_id=requested_fill_id, population_id=population_id,
                                properties_id=properties_id,
                                aggregations_id=aggregations_id,
                                label_lang=label_lang)
    except ValueError as ve:
        errors['metrics'] = str(ve)

    # convert table rows to jsonable dict
    meta = {'snapshot': str(requested_fill_date),
            'population': population_name,
            'population_corrected': population_corrected,
            'label_lang': label_lang,
            'bias': bias,
            'bias_property': bias_property,
            'aggregation_properties': [Properties(p).name for p in properties_id.properties]}
    if represented_biases:
        meta['bias_labels'] = represented_biases
    full_response = {'meta': meta, 'metrics': metrics}
    return jsonify(**full_response)


if __name__ == "__main__":
    app.run()
