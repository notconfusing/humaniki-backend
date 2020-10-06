from flask import Flask, abort, jsonify, request
from humaniki_schema.schema import metric, metric_aggregations_j, metric_aggregations_n, metric_coverage, metric_properties_j, metric_properties_n, project, human_property, human, human_country, human_occupation, human_sitelink
from humaniki_schema.utils import PopulationDefinition, FillType

from humaniki_schema.db import session_factory
from flask_sqlalchemy_session import flask_scoped_session

app = Flask(__name__)
session = flask_scoped_session(session_factory, app)

@app.route("/v1/gender/gap/<string:snapshot>/<string:population>/")
def gap(snapshot, population):
    #TODO
    # assert that the snapshot is 'latest' or a date.
    # assert that the population is one of the options
    # assert that the query-keys are all in
    # - ['country', 'year_of_birth_start', 'year_of_birth_end',
    #     'occupation', 'project']
    # set population correctly if there is project/population conflict
    # get the properties ID based on the properties or return Error
    # metric associated with the properties, population, and fill
    # JSONify the request
    query_values = request.values
    print(query_values)
    return jsonify(**query_values)

if __name__ == "__main__":
    app.run()

