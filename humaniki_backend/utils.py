from datetime import datetime

from humaniki_schema.queries import get_exact_fill_id
from humaniki_schema import utils
from humaniki_schema.schema import metric_properties_j, metric_properties_n
from humaniki_schema.utils import Properties, make_fill_dt, HUMANIKI_SNAPSHOT_DATE_FMT


def get_pid_from_str(property_str):
    try:
        internal_prop_val = getattr(Properties, property_str.upper()).value
        return internal_prop_val
    except AttributeError:
        return None


def order_query_params(query_params):
    # first have to match the properties to their to numbers
    pid_val = {}
    non_orderable_params = {}
    for p_str, val in query_params.items():
        property = get_pid_from_str(p_str)
        if isinstance(property, int):
            pid_val[property] = val
        else:
            non_orderable_params[p_str] = val

    # pid_val = {get_pid_from_str(p_str): val for p_str, val in query_params.items() if get_pid_from_str(p_str)}
    sorted_pids = sorted(pid_val.keys())
    sorted_pid_val = {}
    # In Python 3.7 dictionaries keep insertion order https://stackoverflow.com/a/40007169
    for pid in sorted_pids:
        sorted_pid_val[pid] = pid_val[pid]
    return sorted_pid_val, non_orderable_params


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
    # assert that label_lang is valid
    return True


def determine_population_conflict(population, query_params):
    '''
    :param population: as string that matches humaniki_schema.utils PopulationDefinitiion
    :param query_params: the query string parameters
    :return: (popuatlion_id, population_name, was_corrected) tuple
    '''
    # set population correctly if there is project/population conflict
    # and return population as an id
    # a conflict occurs when the populations is set to all-wikidata and project is specified
    was_corrected = False
    if (population == 'all_wikidata') and ('project' in query_params):
        was_corrected = True
        pop = utils.PopulationDefinition.GTE_ONE_SITELINK
        return pop.value, pop.name, was_corrected
    else:
        pop = getattr(utils.PopulationDefinition, population.upper())
        return pop.value, pop.name, was_corrected


def determine_fill_id(session, snapshot, latest_fill_id, latest_fill_dt):
    """
    figure out the fill id, given a string "latest" or a date in HUMANIKI_SNAPSHOT_DATE_FMT
    :param snapshot:
    :param latest_fill_id:
    :return:
    """
    was_corrected = False
    if snapshot.lower() == 'latest':
        return latest_fill_id, latest_fill_dt, was_corrected
    else:
        try:
            exact_fill_dt = make_fill_dt(snapshot)
        except ValueError as ve:
            raise ValueError(f'snapshot needs to be in {HUMANIKI_SNAPSHOT_DATE_FMT}, not {ve}')
        fill_id, fill_date = get_exact_fill_id(session, exact_fill_dt)
        if fill_id:
            return fill_id, fill_date, was_corrected
        else:
            raise NotImplementedError(
                f'There is no snapshot exactly matching {snapshot} and closes-snapshots arent yet implemented')
            # was_corrected = True
            # return corrected_fill_id, corrected_fill_date, was_corrected

def is_property_exclusively_citizenship(properties_obj):
    if isinstance(properties_obj, metric_properties_j):
        return (properties_obj.properties_len == 1) and (properties_obj.properties[0] == utils.Properties.CITIZENSHIP.value)
    elif isinstance(properties_obj, metric_properties_n):
        raise NotImplementedError
