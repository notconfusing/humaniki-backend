import json
import os
import tempfile

import pytest

from humaniki_schema import generate_example_data
from humaniki_backend import app


skip_generation = os.getenv('HUMANIKI_TEST_SKIPGEN', False)
if not skip_generation:
    generate_example_data.generate_all(data_dir=os.getenv('HUMANIKI_EXMAPLE_DATADIR'),
                                       example_len=10,
                                       num_fills=1)


@pytest.fixture
def test_jsons():
    test_files = {}
    test_datadir = os.environ["HUMANIKI_TEST_DATADIR"]
    files = os.listdir(test_datadir)
    json_fs = [f for f in files if f.endswith('.json')]
    for json_f in json_fs:
        j = json.load(open(os.path.join(test_datadir, json_f)))
        test_files[json_f] = j
    return test_files

@pytest.fixture
def client():
    app.app.config['TESTING'] = True

    with app.app.test_client() as client:
        with app.app.app_context():
            yield client


def test_root_response(client):
    rv = client.get('/')
    json_data = rv.get_json()
    assert json_data is not None

def test_by_language_all(client, test_jsons):
    rv = client.get('/v1/gender/gap/latest/all_wikidata/properties?project=all')
    resp = rv.get_json()
    resp_snap_key = list(resp.keys())[0]
    expected_json = test_jsons['properties_all.json']
    expected_snap_key = list(expected_json.keys())[0]
    population_key = 'GTE_ONE_SITELINK'
    actual_data = resp[resp_snap_key][population_key]
    expected_data = expected_json[expected_snap_key][population_key]
    assert len(actual_data) == len(expected_data)

def test_by_language_enwiki_en(client, test_jsons):
    rv = client.get('/v1/gender/gap/latest/all_wikidata/properties?project=enwiki&label_lang=en')
    resp = rv.get_json()
    resp_snap_key = list(resp.keys())[0]
    expected_json = test_jsons['properties_enwiki_en.json']
    expected_snap_key = list(expected_json.keys())[0]
    population_key = 'GTE_ONE_SITELINK'
    actual_data = resp[resp_snap_key][population_key]
    expected_data = expected_json[expected_snap_key][population_key]
    assert len(actual_data) == len(expected_data)
    assert 'English Wikipedia' in actual_data

def test_by_language_enwiki_fr(client, test_jsons):
    rv = client.get('/v1/gender/gap/latest/all_wikidata/properties?project=enwiki&label_lang=fr')
    resp = rv.get_json()
    resp_snap_key = list(resp.keys())[0]
    expected_json = test_jsons['properties_enwiki_fr.json']
    expected_snap_key = list(expected_json.keys())[0]
    population_key = 'GTE_ONE_SITELINK'
    actual_data = resp[resp_snap_key][population_key]
    expected_data = expected_json[expected_snap_key][population_key]
    actual_data_first_item_key = list(actual_data.keys())[0]
    assert len(actual_data) == len(expected_data)
    assert 'masculin' in actual_data[actual_data_first_item_key]
    assert actual_data[actual_data_first_item_key]['masculin'] == 10
