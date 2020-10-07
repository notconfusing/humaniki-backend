from datetime import datetime

from humaniki_schema import utils


def assert_gap_request_valid(snapshot, population, query_values):
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


def determine_population_conflict(population, query_values):
    # set population correctly if there is project/population conflict
    return None

def build_gap_response(metrics):
    return metrics
