from humaniki_schema.schema import metric, metric_aggregations_j, metric_properties_j, fill

from sqlalchemy import func


def get_properties_id(session, ordered_query_params, bias_property):
    # get the properties ID based on the properties or return Error
    ordered_properties = ordered_query_params.keys()
    properties_id_q = session.query(metric_properties_j.id) \
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


def get_exact_fill_id(session, exact_fill_dt):
    q = session.query(fill.id, fill.date).filter(fill.date == exact_fill_dt)
    fill_id, fill_date = q.one()
    return fill_id, fill_date
