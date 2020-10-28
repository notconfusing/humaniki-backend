import json
import os
import time

import pytest
from sqlalchemy import func

from humaniki_schema import generate_example_data, db
from humaniki_backend import app
from humaniki_schema.schema import metric
from humaniki_schema.utils import read_config_file

config = read_config_file(os.environ['HUMANIKI_YAML_CONFIG'], __file__)

# TODO. If you generate the data seperately and then run the tests they pass. But if you ask the data
# to be generated here, sometimes there are no metrics created, despite, metrics count showing nonzero.
# a mystery.
skip_generation = config['test']['skip_gen'] if 'skip_gen' in config['test'] else False
if not skip_generation:
    generated = generate_example_data.generate_all(config=config)
    print(f'generated: {generated}')
    session = db.session_factory()
    metrics_count = session.query(func.count(metric.fill_id)).scalar()
    print(f'number of metrics: {metrics_count}')
    assert metrics_count>0

@pytest.fixture
def test_jsons():
    test_files = {}
    test_datadir = config['test']['test_datadir']
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
    assert expected_json['meta']['population'] == 'GTE_ONE_SITELINK'
    assert expected_json['meta']['population_corrected'] == True
    actual_data = resp['metrics']
    expected_data = expected_json['metrics']
    assert len(actual_data) == len(expected_data)

def test_by_language_enwiki_en(client, test_jsons):
    rv = client.get('/v1/gender/gap/latest/all_wikidata/properties?project=enwiki&label_lang=en')
    resp = rv.get_json()
    expected_json = test_jsons['properties_enwiki_en.json']
    actual_data = resp['metrics']
    expected_data = expected_json['metrics']
    assert len(actual_data) == len(expected_data)
    the_only_item = actual_data[0]
    assert the_only_item['item_label']['project'] == 'English Wikipedia'
    assert the_only_item['values']['6581097']==10


def test_by_language_enwiki_fr(client, test_jsons):
    rv = client.get('/v1/gender/gap/latest/all_wikidata/properties?project=enwiki&label_lang=fr')
    resp = rv.get_json()
    expected_json = test_jsons['properties_enwiki_fr.json']
    actual_data = resp['metrics']
    actual_meta = resp['meta']
    expected_data = expected_json['metrics']
    assert len(actual_data) == len(expected_data)
    the_only_item = actual_data[0]
    assert 'masculin' in actual_meta['bias_labels'].values()
    assert resp['meta']['label_lang'] == 'fr'

def test_citizenship(client, test_jsons):
    rv = client.get('http://127.0.0.1:5000/v1/gender/gap/latest/gte_one_sitelink/properties?citizenship=all')
    resp = rv.get_json()
    expected_json = test_jsons['properties_citizenship.json']
    actual_data = resp['metrics']
    actual_meta = resp['meta']
    expected_data = expected_json['metrics']
    expected_meta = expected_json['meta']
    assert len(actual_data) == len(expected_data)
    actual_fr_item = [metric for metric in actual_data if metric ['item']['citizenship']=='142'][0]
    expected_fr_item = [metric for metric in expected_data if metric ['item']['citizenship']=='142'][0]
    assert 'iso_3166' in actual_fr_item['item_label'].keys()
    assert actual_fr_item['item_label']['iso_3166'] == expected_fr_item ['item_label']['iso_3166'] == 'FR'

def test_by_dob(client, test_jsons):
    rv = client.get('/v1/gender/gap/latest/gte_one_sitelink/properties?date_of_birth=all&label_lang=en')
    resp = rv.get_json()
    expected_json = test_jsons['properties_dob.json']
    actual_data = resp['metrics']
    actual_meta = resp['meta']
    expected_data = expected_json['metrics']
    expected_meta = expected_json['meta']
    assert len(actual_data) == len(expected_data)
    assert len(actual_meta['bias_labels']) == len(expected_meta['bias_labels'])
    assert actual_meta['bias_labels']['6581097'] == expected_meta['bias_labels']['6581097']
