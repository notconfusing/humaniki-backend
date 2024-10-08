import time

from sqlalchemy.orm import aliased

from humaniki_backend.utils import is_property_exclusively_citizenship, transform_ordered_aggregations_with_year_fns, \
    transform_ordered_aggregations_with_proj_internal_codes, get_transform_ordered_aggregation_qid_match
from humaniki_schema import utils
from humaniki_schema.queries import get_aggregations_obj
from humaniki_schema.schema import metric, metric_aggregations_j, metric_properties_j, label, label_misc, \
    metric_aggregations_n, fill, metric_coverage

from sqlalchemy import func, and_, desc, text

import pandas as pd

from humaniki_schema.utils import Properties, get_enum_from_str
from humaniki_schema.log import get_logger

log = get_logger(BASE_DIR=__file__)


def get_aggregations_id_preds(session, ordered_aggregations, non_orderable_params, as_subquery=True):
    '''transform the ordered_aggreation dict of {prop:value} to {prop: sqlalchemy predicate} for use later on
    as subquery. if the value is specified as 'all' then leave untouched.'''
    # note all pred_fns take and transform in place an ordered_aggregations_dict,
    # they also take session (although I would eventually like to remove that, so the whole lookup can happen in one
    # transaction).
    prop_pred_fn_map = {Properties.PROJECT: transform_ordered_aggregations_with_proj_internal_codes,
                        Properties.DATE_OF_BIRTH: transform_ordered_aggregations_with_year_fns,
                        Properties.DATE_OF_DEATH: transform_ordered_aggregations_with_year_fns,
                        Properties.CITIZENSHIP: get_transform_ordered_aggregation_qid_match(Properties.CITIZENSHIP),
                        Properties.OCCUPATION: get_transform_ordered_aggregation_qid_match(Properties.OCCUPATION)}

    ordered_aggregations_preds = ordered_aggregations # we will be overwriting this
    for prop_id, val in ordered_aggregations.items():
        if val.lower() == 'all':
            continue
        else:
            pred_fn = prop_pred_fn_map[Properties(prop_id)]
            ordered_aggregations_preds = pred_fn(ordered_aggregations_preds, session)

    if as_subquery:
        # optimized
        return ordered_aggregations_preds
    else:
        raise AssertionError('please only as subquery for now on.')



def build_metrics(session, fill_id, population_id, properties_id, aggregations_id, label_lang):
    """
    the entry point for building metrics, first querys the database for the metrics in question
    secondly, builds the nested-dict response.
    :param non_orderable_params:
    :param session:
    :param fill_id:
    :param population_id:
    :param properties_id:
    :param aggregations_id:
    :param label_lang:
    :return:
    """
    # query the metrics table
    build_metrics_start_time = time.time()
    metrics, metrics_columns = get_metrics(session, fill_id, population_id, properties_id, aggregations_id, label_lang)
    build_metrics_query_end_time = time.time()

    # make a nested dictionary represented the metrics
    metrics_response, represented_biases = build_gap_response(properties_id, metrics, metrics_columns, label_lang,
                                                              session)
    build_metrics_grouping_end_time = time.time()

    # timing
    query_metrics_seconds_taken = build_metrics_query_end_time - build_metrics_start_time
    group_metrics_seconds_taken = build_metrics_grouping_end_time - build_metrics_query_end_time
    log.debug(f"Querying metrics repsponse took {'%.3f' % query_metrics_seconds_taken} seconds")
    log.debug(f"Grouping metrics repsponse took {'%.3f' % group_metrics_seconds_taken} seconds")
    return metrics_response, represented_biases


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
        elif prop in [Properties.DATE_OF_BIRTH.value, Properties.DATE_OF_DEATH.value]:
            label_table = None  # there is no join to be made
            join_key = None  # there is no join to be made
        else:
            label_table = label
            join_key = 'qid'
        aliased_label = aliased(label_table, name=f"label_{prop_i}") if label_table else None
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
    aliased_label_cols = []
    dimension_label_params = []
    for i, aj in enumerate(aliased_joins):
        if aj['label_table']:
            # the left key from the unlabelled metric
            metrics_subq_join_col = getattr(metrics_subq.c, f'agg_{i}')
            # define the right key
            label_join_table = aj['label_table']
            label_join_key = aj['join_key']
            label_subtable_cols = [getattr(label_join_table, label_join_key),
                                   getattr(label_join_table, 'lang'),
                                   getattr(label_join_table, 'label')]
            label_join_table_lang_filtered = session.query(*label_subtable_cols) \
                .filter(label_join_table.lang == label_lang) \
                .subquery(f'label_sub_{i}')

            # was
            # label_col = aj['label_table'].label.label(f'label_agg_{i}')
            label_col = label_join_table_lang_filtered.c.label.label(f'label_agg_{i}')

            label_join_column = getattr(label_join_table_lang_filtered.c, label_join_key)
            dimension_label_tuple = (label_join_table_lang_filtered, label_join_column, metrics_subq_join_col)
            dimension_label_params.append(dimension_label_tuple)

        else:  # we probably aren't joining, like for labelling years
            label_col = getattr(metrics_subq.c, f'agg_{i}').label(f'label_agg_{i}')
        aliased_label_cols.append(label_col)

    # first there will always be the bias_value to label
    bias_sublabel_table = session.query(label_misc).filter(label_misc.lang == label_lang,
                                                           label_misc.type == 'bias').subquery('label_sub')

    label_query_cols = [metrics_subq, bias_sublabel_table.c.label.label('bias_label'), *aliased_label_cols]
    labelled_q = session.query(*label_query_cols) \
        .outerjoin(bias_sublabel_table,
                   bias_sublabel_table.c.src == metrics_subq.c.bias_value)

    for (label_join_table_lang_filtered, label_join_column, metrics_subq_join_col) in dimension_label_params:
        labelled_q = labelled_q \
            .outerjoin(label_join_table_lang_filtered, label_join_column == metrics_subq_join_col)

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
    if isinstance(aggregations_id, dict):
        for prop_pos, (prop_id, val) in enumerate(aggregations_id.items()):
            if val == 'all':
                continue
            else:
                prop_pos_after_bias = prop_pos + 1
                a_man = aliased(metric_aggregations_n)
                val_predicate = val(a_man.value) if callable(val) else a_man.value == val
                metrics_q = metrics_q.join(a_man, and_(metric.aggregations_id == a_man.id,
                                                       a_man.aggregation_order == prop_pos_after_bias,
                                                       a_man.property == prop_id,
                                                       val_predicate))

    # if a label_lang is defined we need to make a subquery
    if label_lang is not None:
        metrics_subq = metrics_q.subquery('metrics_driver')
        metrics_q = label_metric_query(session, metrics_subq, properties, label_lang)

    log.debug(f'metrics_q is:'
              f' {metrics_q.statement.compile(compile_kwargs={"literal_binds": True})}')
    metrics = metrics_q.all()
    metrics_columns = metrics_q.column_descriptions
    log.debug(f'Number of metrics to return are {len(metrics)}')
    return metrics, metrics_columns


def build_gap_response(properties_id, metrics_res, columns, label_lang, session):
    """
    transforms a metrics response into a json-able serialization
    see https://docs.google.com/document/d/1tdm1Xixy-eUvZkCc02kqQre-VTxzUebsComFYefS5co/edit#heading=h.a8xg7ij7tuqm
    :param label_lang:
    :param metrics:
    :return: response dict
    """
    prop_names = [utils.Properties(p).name.lower() for p in properties_id.properties]
    is_citizenship = is_property_exclusively_citizenship(properties_id)
    iso_codes = get_iso_codes_as_lookup_table(session) if is_citizenship else None
    col_names = [col['name'] for col in columns]
    aggr_cols = [col['name'] for col in columns if col['name'].startswith('agg')]
    label_cols = [col['name'] for col in columns if col['name'].startswith('label')]
    # use pandas to group by all dimensions except gender
    metric_df = pd.DataFrame.from_records(metrics_res, columns=col_names)
    metric_df.to_dict()
    pandas_groupby_start = time.time()
    agg_groups = metric_df.groupby(by=aggr_cols) if aggr_cols else [('global', metric_df)]
    pandas_groupby_end = time.time()
    # accumulator pattern
    data_points = []
    for group_i, (group_name, group) in enumerate(agg_groups):
        group_name_as_list = group_name if isinstance(group_name, tuple) else [group_name]
        item_d = dict(zip(prop_names, group_name_as_list))
        values = dict(group[['bias_value', 'total']].to_dict('split')['data'])
        labels_prop_order = group[label_cols].iloc[0].values if label_cols else []
        item_labels = dict(zip(prop_names, labels_prop_order))
        if is_citizenship:
            try:
                item_labels['iso_3166'] = iso_codes[group_name]
            except KeyError as ke:
                # log.debug(f'iso code exception: {ke}')
                pass
        data_point = {'order': group_i,
                      'item': item_d,
                      'item_label': item_labels,
                      "values": values}
        # including this just once in the meta portion for now.
        # if label_lang:
        #     labels = dict(group[['bias_value', 'bias_label']].to_dict('split')['data'])
        #     data_point['labels'] = labels
        data_points.append(data_point)
    pandas_group_iteration_end = time.time()
    log.debug(f'pandas groupby took {pandas_groupby_end - pandas_groupby_start} seconds')
    log.debug(f'pandas group iteration took {pandas_group_iteration_end - pandas_groupby_end} seconds')

    represented_biases = make_represented_genders(metric_df, label_lang) if label_lang else None

    return data_points, represented_biases


def make_represented_genders(metric_df, label_lang):
    """
    return a dict of the represented genders and  (maybe their label) to make life easy for the front end.
    :return:
    """
    return dict(metric_df[['bias_value', 'bias_label']].drop_duplicates().to_dict('split')['data'])


def get_iso_codes_as_lookup_table(session, iso_subtype='iso_3166_1'):
    """
    :return: a dict mapping qids to iso_3166_1
    """
    iso_codes = session.query(label_misc.src, label_misc.label).filter(label_misc.lang == iso_subtype).all()
    iso_codes_df = pd.DataFrame.from_records(iso_codes)
    iso_codes_dict = iso_codes_df.to_dict('split')['data']
    return dict(iso_codes_dict)


def get_metrics_count(session):
    metrics_count = session.query(func.count(metric.fill_id)).scalar()
    return metrics_count


def get_all_snapshot_dates(session):
    snapshots = session.query(fill).filter(fill.detail['active'] == True).order_by(desc(fill.date)).all()
    snapshot_dicts = [snapshot.to_dict() for snapshot in snapshots]
    return snapshot_dicts


def get_coverage(session, population_id, properties_id, fill_id):
    ## TODO, use sqlalchemy parameters not f-strings to sanitize inputs
    metric_coverage_sql = text(f""" select n.total_with_properties / d.total_with_properties as coverage
                                from
                            (select
                                   total_with_properties,
                                   'n' as nd,
                                    'k' as k
                            from metric_coverage
                            where fill_id={fill_id} and properties_id={properties_id} and population_id={population_id}) n
                            join
                            (select
                                   total_with_properties,
                                   'd' as nd,
                                  'k' as k
                            from metric_coverage
                            where fill_id={fill_id}
                              -- i really hope the fact holds that this is the numerator we want.
                              and properties_id=(select id
                                                from metric_properties_j
                                                where properties_len=0)
                              and population_id={population_id}) d
                            on n.k = d.k
                              """)
    coverage_decimal = session.execute(metric_coverage_sql).scalar()
    try:
        coverage = float(coverage_decimal)
    except TypeError:
        coverage = None
    return coverage
