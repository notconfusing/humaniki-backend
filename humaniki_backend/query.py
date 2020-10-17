import operator
from collections.__init__ import defaultdict
from functools import reduce

from sqlalchemy.orm import aliased

from humaniki_schema import utils
from humaniki_schema.queries import get_aggregations_obj
from humaniki_schema.schema import metric, metric_aggregations_j, metric_properties_j, fill, label, project, label_misc

from sqlalchemy import func

import pandas as pd

def get_aggregations_ids(session, ordered_aggregations):
    # aggregations_id is None indicates there's no constraint on the aggregation_id
    if all([v == 'all' for v in ordered_aggregations.values()]):
        return None
    else:
        aggregation_objs = get_aggregations_obj(bias_value=None, dimension_values=ordered_aggregations,
                             session=session, as_subquery=False, create_if_no_exist=False)
        aggregations_ids = [a.id for a in aggregation_objs]
        return aggregations_ids

def build_metrics(session, fill_id, population_id, properties_id, aggregations_id, label_lang):
    """
    the entry point for building metrics, first querys the database for the metrics in question
    secondly, builds the nested-dict response.
    :param session:
    :param fill_id:
    :param population_id:
    :param properties_id:
    :param aggregations_id:
    :param label_lang:
    :return:
    """
    # query the metrics table
    metrics, metrics_columns = get_metrics(session, fill_id, population_id, properties_id, aggregations_id, label_lang)
    # make a nested dictionary represented the metrics
    metrics_response = build_gap_response(properties_id, metrics, metrics_columns, label_lang)
    return metrics_response


def generate_json_expansion_values(properties):
    property_query_cols = []
    for prop_i, prop in enumerate(properties):
        prop_col = func.json_extract(metric_properties_j.properties, f"$[{prop_i}]").label(f"prop_{prop_i}")
        agg_col = func.json_unquote(func.json_extract(metric_aggregations_j.aggregations, f"$[{prop_i}]")).label(
            f"agg_{prop_i}")
        property_query_cols.append(prop_col)
        property_query_cols.append(agg_col)

    return property_query_cols


def generate_aliased_tables_for_labelling(properties):
    """
    generate a list of dicts defining how to join an aggregation column
    the details needed are the join table and the join_key like
    [{table:label_misc as 'label_0', join_key:'src'}
            ...
                        {{table:label as 'label_n', join_key:'qid'}}]
    note that the table the join table and key are dependent on the property
    :param properties:
    :return:
    """
    aliased_joins = []
    for prop_i, prop in enumerate(properties):
        if prop == 0:  # recall we are faking sitelinks as property 0
            label_table = label_misc
            join_key = 'src'
        else:
            label_table = label
            join_key = 'qid'
        aliased_label = aliased(label_table, name=f"label_{prop_i}")
        join_data = {'label_table': aliased_label, 'join_key': join_key}
        aliased_joins.append(join_data)
    return aliased_joins


def label_metric_query(session, metrics_subq, properties, label_lang):
    """
     So we have the metrics table, exploded into one aggregation per column, but need to join the labels
     we create an alias of the label table per aggregation, and then join
     note that sitelinks must be joined on label_misc.src and
               qids      must be joined on label.qid

    :return: a sqlalchemy query
    """
    # i wish i could compute the alias joins inline in this function rather than upfront, but
    # I believe I need the column names before I can start joining.
    aliased_joins = generate_aliased_tables_for_labelling(properties)
    aliased_label_cols = [aj['label_table'].label.label(f'label_agg_{i}') for i, aj in enumerate(aliased_joins)]

    label_query_cols = [metrics_subq, label.label.label('bias_label'), *aliased_label_cols]
    # first there will always be the bias_value to label
    labelled_q = session.query(*label_query_cols) \
        .outerjoin(label,
                   label.qid == metrics_subq.c.bias_value) \
        .filter(label.lang == label_lang)

    # then there are the aggregation values to label.
    for j, aliased_join in enumerate(aliased_joins):
        # the left key from the unlabelled metric
        metrics_subq_join_col = getattr(metrics_subq.c, f'agg_{j}')
        # define the right key
        label_join_table = aliased_join['label_table']
        label_join_key = aliased_join['join_key']
        label_join_column = getattr(label_join_table, label_join_key)

        #  make a left join to make sure no metrics are being dropped
        # and also the join table needs to be subsetted to the correct langauge
        labelled_q = labelled_q \
            .outerjoin(label_join_table, label_join_column == metrics_subq_join_col) \
            .filter(label_join_table.lang == label_lang)

    return labelled_q


def get_metrics(session, fill_id, population_id, properties_id, aggregations_id, label_lang):
    """
    get the metrics based on population and properties, and optionally the aggregations

    Expands the metrics row from json aggregations.aggregations list
     --> from
    fill_id | population | [prop_0,..prop_n] | [agg_val_0,  .., agg_val_n] | gender | total
    ---> to
    fill_id | population | prop_0 |...| prop_n |agg_val_0 | ... |agg_val_n] | gender | total

    This can be done by writing a dynamics query, based on the fact that we know how many properties
    are being queried by the api user. For instance.
    The reason this transform is necessary here is to facilitate labelling the aggregation values
    using sql joins. That is also tricky because some aggegration values are site-links, and some are
    qids.

    This jiujitsu may be deprecated if we store the aggregations noramlized rather than as json list.
    The problem I was having there was the hetergenous types of the aggregations (sitelinks, str) (qids, int)
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
    property_query_cols = generate_json_expansion_values(properties)

    query_cols = [*property_query_cols, metric.bias_value, metric.total]

    metrics_q = session.query(*query_cols) \
        .join(metric_properties_j, metric.properties_id == metric_properties_j.id) \
        .join(metric_aggregations_j, metric.aggregations_id == metric_aggregations_j.id) \
        .filter(metric.properties_id == prop_id) \
        .filter(metric.fill_id == fill_id) \
        .filter(metric.population_id == population_id) \
        .order_by(metric.aggregations_id)
    if isinstance(aggregations_id, int):
        metrics_q = metrics_q.filter(metric.aggregations_id == aggregations_id)
    if isinstance(aggregations_id, list):
        metrics_q = metrics_q.filter(metric.aggregations_id.in_(aggregations_id))

    # if a label_lang is defined we need to make a subquery
    if label_lang is not None:
        metrics_subq = metrics_q.subquery('metrics_driver')
        metrics_q = label_metric_query(session, metrics_subq, properties, label_lang)

    # print(f'metrics_q is {metrics_q}')
    metrics = metrics_q.all()
    metrics_columns = metrics_q.column_descriptions
    print(f'Number of metrics to return are {len(metrics)}')
    return metrics, metrics_columns


def build_gap_response(properties_id, metrics_res, columns, label_lang):
    """
    transforms a metrics response into a json-able serialization
    see https://docs.google.com/document/d/1tdm1Xixy-eUvZkCc02kqQre-VTxzUebsComFYefS5co/edit#heading=h.a8xg7ij7tuqm
    :param label_lang:
    :param metrics:
    :return: response dict
    """
    prop_names = [utils.Properties(p).name.lower() for p in properties_id.properties]
    col_names = [col['name'] for col in columns]
    aggr_cols = [col['name'] for col in columns if col['name'].startswith('agg')]
    label_cols = [col['name'] for col in columns if col['name'].startswith('label')]
    metric_df = pd.DataFrame.from_records(metrics_res, columns=col_names)
    metric_df.to_dict()
    agg_groups = metric_df.groupby(by=aggr_cols)
    data_points = []
    for group_i, (group_name, group) in enumerate(agg_groups):
        group_name_as_list = group_name if isinstance(group_name, tuple) else [group_name]
        item_d = dict(zip(prop_names, group_name_as_list))
        values = dict(group[['bias_value','total']].to_dict('split')['data'])
        labels = dict(group[['bias_value','bias_label']].to_dict('split')['data'])
        labels_prop_order = group[label_cols].iloc[0].values
        item_labels = dict(zip(prop_names, labels_prop_order))
        data_point = {'order':group_i,
                      'item':item_d,
                      'item_label': item_labels,
                      "values": values,
                      'labels':labels}
        data_points.append(data_point)

    return data_points

    # number_of_aggregations = len(properties_id.properties)
    # # print(f"number_of_aggregations:{number_of_aggregations}")
    # bias_col_name = 'bias_label' if label_lang else 'bias_value'
    # agg_col_prefix = 'agg_label' if label_lang else 'agg'
    # agg_cols = [col['name'] for col in columns if col['name'].startswith(agg_col_prefix)]
    # val_resp_dict = build_layer_default_dict(number_of_aggregations)
    # for row_i, row in enumerate(metrics_res):
    #     resp_dict_path = []
    #     ## get the aggregation_values
    #     agg_vals = [getattr(row, agg_col) for agg_col in agg_cols]
    #     resp_dict_path.extend(agg_vals)
    #     resp_dict_path.append(getattr(row, bias_col_name))
    #     set_dict_path(val_resp_dict, resp_dict_path, row.total)
    #     # item_d = [getattr(row, f'prop_{p}') for p in number_of_aggregations]
    # return


def get_latest_fill_id(session):
    latest_q = session.query(func.max(fill.date)).subquery()
    q = session.query(fill.id, fill.date).filter(fill.date == latest_q)
    latest_fill_id, latest_fill_date = q.one()
    return latest_fill_id, latest_fill_date


def get_exact_fill_id(session, exact_fill_dt):
    q = session.query(fill.id, fill.date).filter(fill.date == exact_fill_dt)
    fill_id, fill_date = q.one()
    return fill_id, fill_date

def get_metrics_count(session):
    metrics_count = session.query(func.count(metric.fill_id)).scalar()
    return metrics_count

# def build_layer_default_dict(n):
#     """
#     Build an n-level defaultdict    not sure if this is clever or unnecessary brain surgery, i'll never undestand again
#     :param n: levels
#     :return: a very-defaultdict
#     """
#     ret = defaultdict(dict)
#     for layer in range(1, n):
#         ret = defaultdict(lambda: ret)
#     return ret
#
#
# def get_dict_path(dct, key_path):
#     '''
#     :param dct: an n-nested dict of dicts
#     :param key_path: a list of keys
#     :return:
#     '''
#     return reduce(operator.getitem, key_path, dct)
#
#
# def set_dict_path(dct, key_path, value):
#     """
#     :param dct:  an n-nested dict of dicts
#     :param key_path: a list of keys
#     :param value: a value to set a path location
#     :return:
#     """
#     get_dict_path(dct, key_path[:-1])[key_path[-1]] = value
#
