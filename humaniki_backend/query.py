from humaniki_schema.utils import PopulationDefinition, FillType, Properties
from humaniki_schema.schema import metric, metric_aggregations_j, metric_aggregations_n, metric_coverage, \
    metric_properties_j, metric_properties_n, project, human_property, human, human_country, human_occupation, \
    human_sitelink, fill

from sqlalchemy import func

def get_properties_id(session, query_values):
    # get the properties ID based on the properties or return Error
    query_keys = [k.upper() for k in query_values.keys()]
    properties = [getattr(Properties, k) for k in query_keys if k in Properties.__members__]
    properties_ints = [prop.value for prop in properties]
    ordered_properties = sorted(properties_ints)
    properties_id_q = session.query(metric_properties_j.id)
    for pos, prop_num in enumerate(ordered_properties):
        properties_id_q = properties_id_q.filter(metric_properties_j.properties[pos]==prop_num)
    print(f"Properties query {properties_id_q}")
    properties_id = properties_id_q.all()
    print(f"Properties id is: {properties_id}")
    return properties_id


def get_metrics(session, fill_id, population, properties_id, aggregations_id, use_lang='en'):
    # get the properties ID based on the properties or return Error
    properties_id = None
    return properties_id


def get_latest_fill_id(session):
    latest_q = session.query(func.max(fill.date)).subquery()

    q = session.query(fill.id, fill.date).filter(fill.date==latest_q)

    latest_fill_id, latest_fill_date = q.one()
    return latest_fill_id, latest_fill_date
