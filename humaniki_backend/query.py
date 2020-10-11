import operator
from collections.__init__ import defaultdict
from functools import reduce

from sqlalchemy.orm import aliased

from humaniki_schema.schema import metric, metric_aggregations_j, metric_properties_j, fill, label, project

from sqlalchemy import func


def get_properties_id(session, ordered_query_params, bias_property):
    # get the properties ID based on the properties or return Error
    ordered_properties = ordered_query_params.keys()
    properties_id_q = session.query(metric_properties_j.id, metric_properties_j.properties) \
        .filter(metric_properties_j.bias_property == bias_property) \
        .filter(metric_properties_j.properties_len == len(ordered_properties))
    for pos, prop_num in enumerate(ordered_properties):
        print(pos, prop_num)
        properties_id_q = properties_id_q.filter(metric_properties_j.properties[pos] == prop_num)
    # print(f"Properties query {properties_id_q}")
    # TODO see if using subqueries is faster
    properties_id_subquery = properties_id_q.subquery()
    properties_id_obj = properties_id_q.one()
    properties_id_int = properties_id_obj.id
    # print(f"Properties id is: {properties_id_int}")
    return properties_id_obj


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


def build_metrics(session, fill_id, population_id, properties_id, aggregations_id, label_lang='en'):
    metrics, metrics_columns = get_metrics(session, fill_id, population_id, properties_id, aggregations_id, label_lang)
    metrics_response = build_gap_response(properties_id, metrics, metrics_columns)
    return metrics_response

def get_metrics(session, fill_id, population_id, properties_id, aggregations_id, label_lang):
    """
    get the metrics based on population and properties, and optionally the aggregations
    :param session:
    :param fill_id:
    :param population_id:
    :param properties_id:
    :param aggregations_id: a specificed aggregations id, or None
    :param label_lang: if not None then label
    :return:
    """
    prop_id = properties_id.id
    properties = properties_id.properties

    property_query_cols = []
    aggregation_query_cols = []
    aliased_labels = []
    aliased_join_keys = []
    for prop_i, prop in enumerate(properties):
        prop_col = func.json_extract(metric_properties_j.properties, f"$[{prop_i}]").label(f"prop_{prop_i}")
        agg_col = func.json_unquote(func.json_extract(metric_aggregations_j.aggregations, f"$[{prop_i}]")).label(f"agg_{prop_i}")
        property_query_cols.append(prop_col)
        property_query_cols.append(agg_col)

        # TODO choose specific label tables
        label_table = None
        if prop == 0:  # recall we are faking sitelinks as property 0
            label_table = project
            join_key = 'code'
        else:
            label_table = label
            join_key = 'qid'
        aliased_label = aliased(label_table, name=f"label_{prop_i}")
        aliased_labels.append(aliased_label)
        aliased_join_keys.append(join_key)

    query_cols = [*property_query_cols, *aggregation_query_cols, metric.bias_value, metric.total]

    metrics_q = session.query(*query_cols) \
        .join(metric_properties_j, metric.properties_id == metric_properties_j.id) \
        .join(metric_aggregations_j, metric.aggregations_id == metric_aggregations_j.id) \
        .filter(metric.properties_id == prop_id) \
        .filter(metric.fill_id == fill_id) \
        .filter(metric.population_id == population_id) \
        .order_by(metric.aggregations_id)
    if isinstance(aggregations_id, int):
        metrics_q = metrics_q.filter(metric.aggregations_id == aggregations_id)
    metrics_subq = metrics_q.subquery()

    aliased_label_cols = [al.label.label(f'agg_label_{i}') for i, al in enumerate(aliased_labels)]
    labelled_query_cols = [*aliased_label_cols, metric.bias_value, metric.total]
    labelled_q = session.query(metrics_subq, label.label.label('bias_label'),  *aliased_label_cols) \
        .outerjoin(aliased_labels[0],
                   getattr(aliased_labels[0], aliased_join_keys[0]) == metrics_subq.c.agg_0)\
        .outerjoin(label,
                   label.qid==metrics_subq.c.bias_value)\
        .filter(label.lang==label_lang)

    print(f'metrics_q is {labelled_q}')

    metrics = labelled_q.all()
    metrics_columns = labelled_q.column_descriptions
    # metrics = metrics_q.all()
    print(f'Number of metrics to return are {len(metrics)}')
    return metrics, metrics_columns


def build_gap_response(properties_id, metrics_res, columns):
    """
    transforms a metrics response into a json-able serialization
    :param metrics:
    :return: response dict
    """
    # TODO need to exclude the bias-values from the aggregations
    number_of_aggregations = len(properties_id.properties)
    print(f"number_of_aggregations:{number_of_aggregations}")
    resp_dict = build_layer_default_dict(number_of_aggregations)
    agg_cols = [col['name'] for col in columns if col['name'].startswith('agg_label')]
    for row in metrics_res:
        resp_dict_path = []
        ## get the aggregation_values
        agg_vals = [getattr(row, agg_col) for agg_col in agg_cols]
        ##
        resp_dict_path.extend(agg_vals)
        resp_dict_path.append(row.bias_label)
        set_dict_path(resp_dict, resp_dict_path, row.total)
    return resp_dict


def get_latest_fill_id(session):
    latest_q = session.query(func.max(fill.date)).subquery()
    q = session.query(fill.id, fill.date).filter(fill.date == latest_q)
    latest_fill_id, latest_fill_date = q.one()
    return latest_fill_id, latest_fill_date


def get_exact_fill_id(session, exact_fill_dt):
    q = session.query(fill.id, fill.date).filter(fill.date == exact_fill_dt)
    fill_id, fill_date = q.one()
    return fill_id, fill_date


def build_layer_default_dict(n):
    """
    Build an n-level defaultdict    not sure if this is clever or unnecessary brain surgery, i'll never undestand again
    :param n: levels
    :return: a very-defaultdict
    """
    ret = defaultdict(dict)
    for layer in range(1, n):
        ret = defaultdict(lambda: ret)
    return ret


def get_dict_path(dct, key_path):
    '''
    :param dct: an n-nested dict of dicts
    :param key_path: a list of keys
    :return:
    '''
    return reduce(operator.getitem, key_path, dct)


def set_dict_path(dct, key_path, value):
    """
    :param dct:  an n-nested dict of dicts
    :param key_path: a list of keys
    :param value: a value to set a path location
    :return:
    """
    get_dict_path(dct, key_path[:-1])[key_path[-1]] = value
