from datetime import datetime
from collections import defaultdict
from functools import reduce  # forward compatibility for Python 3
import operator

from humaniki_schema import utils


def assert_gap_request_valid(snapshot, population, query_params):
    # assert that the snapshot is 'latest' or a date.
    snapshot_equal_latest = snapshot == 'latest'
    try:
        snapshot_dt = datetime.strptime(snapshot, utils.HUMANIKI_SNAPSHOT_DATE_FMT)
    except ValueError as e:
        snapshot_dt = False
    assert snapshot_equal_latest or snapshot_dt
    # assert that the population is one of the options
    # assert that the query-keys are all in
    # - ['country', 'year_of_birth_start', 'year_of_birth_end',
    #     'occupation', 'project']
    return True


def determine_population_conflict(population, query_params):
    '''
    :param population: as string that matches humaniki_schema.utils PopulationDefinitiion
    :param query_params: the query string parameters
    :return: (popuatlion_id, was_corrected) tuple
    '''
    # set population correctly if there is project/population conflict
    # and return population as an id
    # a conflict occurs when the populations is set to all-wikidata and project is specified
    was_corrected = False
    if (population == 'all_wikidata') and ('project' in query_params):
        was_corrected = True
        return utils.PopulationDefinition.GTE_ONE_SITELINK.value, was_corrected
    else:
        return getattr(utils.PopulationDefinition, population.upper()).value, was_corrected


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


def build_gap_response(metrics_res):
    """
    transforms a metrics response into a json-able serialization
    :param metrics:
    :return: response dict
    """
    # TODO need to exclude the bias-values from the aggregations
    first_res_third_sql_alchemy_obj = metrics_res[0][2]
    number_of_aggregations = len(first_res_third_sql_alchemy_obj.aggregations)
    print(f"number_of_aggregations:{number_of_aggregations}")
    resp_dict = build_layer_default_dict(number_of_aggregations)
    for metric_obj, properties_obj, aggregation_obj in metrics_res:
        # if aggregation_obj.aggregations['facets'][0]=='enwiki':
        #     print(f'aggregations are;{aggregation_obj.aggregations}')
        resp_dict_path = []
        resp_dict_path.extend(aggregation_obj.aggregations)
        resp_dict_path.append(metric_obj.bias_value)
        set_dict_path(resp_dict, resp_dict_path, metric_obj.total)
    return resp_dict
