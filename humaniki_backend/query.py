from humaniki_schema.utils import PopulationDefinition, FillType, Properties
from humaniki_schema.schema import metric, metric_aggregations_j, metric_aggregations_n, metric_coverage, \
    metric_properties_j, metric_properties_n, project, human_property, human, human_country, human_occupation, \
    human_sitelink, fill

from sqlalchemy import func


def get_pid_from_str(property_str):
    return getattr(Properties, property_str.upper()).value


def order_query_params(query_params):
    # first have to match the properties to their to numbers
    pid_val = {get_pid_from_str(p_str): val for p_str, val in query_params.items()}
    sorted_pids = sorted(pid_val.keys())
    sorted_pid_val = {}
    # In Python 3.7 dictionaries keep insertion order https://stackoverflow.com/a/40007169
    for pid in sorted_pids:
        sorted_pid_val[pid] = pid_val[pid]
    return sorted_pid_val


def get_properties_id(session, ordered_query_params, bias_property):
    # get the properties ID based on the properties or return Error
    ordered_properties = ordered_query_params.keys()
    properties_id_q = session.query(metric_properties_j.id)\
        .filter(metric_properties_j.bias_property == bias_property) \
        .filter(metric_properties_j.properties_len == len(ordered_properties))
    for pos, prop_num in enumerate(ordered_properties):
        print(pos, prop_num)
        properties_id_q = properties_id_q.filter(metric_properties_j.properties[pos] == prop_num)
    print(f"Properties query {properties_id_q}")
    # TODO see if using subqueries is faster
    properties_id_subquery = properties_id_q.subquery()
    properties_id = properties_id_q.all()
    properties_id_int = properties_id[0][0]
    print(f"Properties id is: {properties_id_int}")
    return properties_id_int


def get_aggregations_ids(session, ordered_query_params):
    # aggregations_id is None indicates there's no constraint on the aggregation_id
    ordered_aggregations = ordered_query_params.values()
    if all([v == 'all' for v in ordered_aggregations]):
        return None
    else:
        aggregations_id_q = session.query(metric_aggregations_j.id)
        for pos, agg_val in enumerate(ordered_aggregations):
            if agg_val != 'all':  # hope there is no value called all
                aggregations_id_q = aggregations_id_q.filter(metric_aggregations_j.aggregations[pos] == agg_val)
        print(f"aggregations query {aggregations_id_q}")
        # TODO see if using subqueries is faster
        aggregations_id_subquery = aggregations_id_q.subquery()
        aggregations_id = aggregations_id_q.all()
        print(f"aggregations_id is: {aggregations_id}")
        return aggregations_id


def get_metrics(session, fill_id, population_id, properties_id, aggregations_id, use_lang='en'):
    # get the properties ID based on the properties or return Error.
    metrics_q = session.query(metric, metric_properties_j, metric_aggregations_j) \
        .join(metric_properties_j, metric.properties_id == metric_properties_j.id) \
        .join(metric_aggregations_j, metric.aggregations_id == metric_aggregations_j.id) \
        .filter(metric.properties_id == properties_id) \
        .filter(metric.fill_id == fill_id) \
        .filter(metric.population_id == population_id) \
        .order_by(metric.aggregations_id)
    if isinstance(aggregations_id, int):
        metrics_q = metrics_q.filter(metric.aggregations_id == aggregations_id)
    elif isinstance(aggregations_id, list):
        metrics_q = metrics_q.filter(metric.aggregations_id.in_(aggregations_id))
    print(f'metrics_q is {metrics_q}')
    metrics = metrics_q.all()
    print(f'Number of metrics to return are {len(metrics)}')
    return metrics


def get_latest_fill_id(session):
    latest_q = session.query(func.max(fill.date)).subquery()

    q = session.query(fill.id, fill.date).filter(fill.date == latest_q)

    latest_fill_id, latest_fill_date = q.one()
    return latest_fill_id, latest_fill_date
