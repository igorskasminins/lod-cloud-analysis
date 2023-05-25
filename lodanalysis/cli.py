""" Module for defining CLI commands  """

from lodanalysis.config import Config
from lodanalysis.collection_dump import CollectionDump
from lodanalysis.lod_cloud import LODCloud
from lodanalysis.mongo_db import DB
from lodanalysis.sparql_data_extractor import SPARQLDataExtractor
from lodanalysis.sparql_queries import SPARQLQueries
import typer

app = typer.Typer()
db = DB()
config = Config()
collection_dump = CollectionDump()
lod_cloud = LODCloud()
data_extractor = SPARQLDataExtractor()

@app.command()
def generate(
    include_base_queries: bool = typer.Option(
        True,
        confirmation_prompt=True,
        prompt='Include base queries (triples, classes and properties)?'
    ),
    queries_directory: str = typer.Option(
        '',
        '--input-file',
        '-i',
        prompt='Directory with queries (Leave empty to skip)'
    )
) -> None:
    """ Extracts data from the LOD Cloud JSON file and performs SPARQL queries on their endpoints """
    if not config.check_dir(queries_directory):
        print('The specified directory does not exist or is empty')
        return

    process_result = lod_cloud.process_data(
        include_base_queries,
        queries_directory
    )
    if (process_result == False):
        print('An error has occured while trying to get the LOD Cloud JSON file')
        return
    else:
        print('The LOD Cloud processing has been finished!')

@app.command('generate-custom')
def generate_custom_queries(
    only_new_custom_queries: bool = typer.Option(
        True,
        confirmation_prompt=True,
        prompt='Update only with not existing queries (existing queries\' results will not be overwrtitten)?'
    ),
    queries_directory: str = typer.Option(
        None,
        '--input-file',
        '-i',
        prompt='Queries directory'
    )
) -> None:
    """ Performs custom queries on existing stored active endpoint; appends new or replaces all existing result based on the query names """
    if not config.check_dir(queries_directory):
        print('The specified directory does not exist or empty')
        return

    active_endpoints = db.get_endpoint_collection({DB.STATUS: DB.STATUS_OK})
    for endpoint in active_endpoints:
        print(endpoint[DB.ACCESS_URL])
        updated_endpoint = data_extractor.extract_data(
            endpoint[DB.ACCESS_URL],
            include_base_queries=False,
            queries_directory=queries_directory,
            only_new_custom_queries=only_new_custom_queries,
        )

        db.update_endpoint(updated_endpoint)
        
@app.command()
def get(
    access_url: str = typer.Option(
        None,
        '--access-url',
        '-url',
        prompt='SPARQL Endpoint Access URL'
    ),
    include_base_queries: bool = typer.Option(
        True,
        confirmation_prompt=True,
        prompt='Include base queries (triples, classes and properties)?'
    ),
    queries_directory: str = typer.Option(
        '',
        '--queries-directory',
        '-q',
        prompt='Directory with queries (leave the field empty to skip custom queries)'
    ),
    output_file_name: str = typer.Option(
        config.get_file_config('endpoint_dump'),
        '--output-file',
        '-o',
        prompt='Output file'
    )
) -> None:
    """ Analyzes a single SPARQL endpoint by its URL and performs custom queries """
    endpoint_data = data_extractor.extract_data(
        access_url=access_url,
        include_base_queries=include_base_queries,
        queries_directory=queries_directory,
        save_endpoint=False,
        only_new_custom_queries=False
    )

    collection_dump.export_dump(output_file_name, endpoint_data)

@app.command()
def download() -> None:
    """ Downloads the latest LOD cloud JSON file with raw datasets """
    input_file = config.get_file_config('raw_data') + '.json'

    lod_cloud.get_lod_cloud_json(input_file)

@app.command()
def dump(
    output_file_name: str = typer.Option(
        config.get_file_config('collection_dump'),
        '--output-file',
        '-o',
        prompt='Output file'
    ),
    only_active: bool = typer.Option(
        True,
        confirmation_prompt=True,
        prompt='Include all endpoints? (the failed endpoint will not be skipped)'
    )
) -> None:
    """ Dumps the whole endpoint collection in JSON format """
    filters = {}
    if only_active == True:
        filters[DB.STATUS] = DB.STATUS_OK

    collection = db.get_endpoint_collection(filters)
    collection_dump.export_dump(output_file_name, collection)

    print('Data dump has been created!')

@app.command('top-properties')
def top_properties(
    separate: bool = typer.Option(
        True,
        '--separate-domains',
        '-d',
        prompt='Separate by domains?'),
    output_file_name: str = typer.Option(
        config.get_file_config('top_properties_dump'),
        '--output-file',
        '-o',
        prompt='Output dump file name'
    )
) -> None:
    """ Retrieves the most used properties accross all endpoints """
    if separate == True:
        for domain in db.get_domains():
            domain_name = domain['_id']
            most_used_props = db.get_most_used_instances(DB.USED_PROPERTIES, domain_name)
            collection_dump.export_dump(output_file_name + '_' + domain_name, most_used_props)
    else:
        result = db.get_most_used_instances(DB.USED_PROPERTIES)
        collection_dump.export_dump(output_file_name, result)

@app.command('top-classes')
def top_classes(
    separate: bool = typer.Option(
        True,
        '--separate-domains',
        '-d',
        prompt='Separate by domains?'),
    output_file_name: str = typer.Option(
        config.get_file_config('top_classes_dump'),
        '--output-file',
        '-o',
        prompt='Output dump file name'
    )
) -> None:
    """ Retrieves the most used classes accross all endpoints """
    if separate == True:
        for domain in db.get_domains():
            domain_name = domain['_id']
            most_used_classes = db.get_most_used_instances(DB.USED_CLASSES, domain_name)
            collection_dump.export_dump(output_file_name + '_' + domain_name, most_used_classes)
    else:
        result = db.get_most_used_instances(DB.USED_CLASSES)
        collection_dump.export_dump(output_file_name, result)

@app.command('delete-query')
def delete_query(
    query_name: str = typer.Option(
        None,
        '--query',
        '-q',
        prompt='Query name'
    )
) -> None:
    """ Delete results from a single query accross all endpoints """
    db.delete_queries([query_name])
    print('The querie\'s result have been removed')

@app.command('delete-dir-queries')
def delete_directory_queries(
    queries_directory: str = typer.Option(
        None,
        '--dir',
        '-d',
        prompt='Directory name'
    )
) -> None:
    """ Delete results from all queries in the specified directory accross all endpoints """
    if not config.check_dir(queries_directory):
        print('The specified directory does not exist or empty')
        return
    
    queries = config.get_custom_queries_names(queries_directory)
    db.delete_queries(queries)

    print('The queries\' results from the directory have been removed')

@app.command('delete')
def delete_endpoint(
    access_url: str = typer.Option(
        None,
        '--access-url',
        '-url',
        prompt='Endpoint access URL'
    )) -> None:

    db.delete_endpoint(access_url)

@app.command('drop')
def drop_collections(delete_collections: bool = typer.Option(
        True,
        confirmation_prompt=True,
        prompt='Are you sure you want to delete all collections?'
    )
) -> None:
    """ Drops the endpoint, property and class collection """
    if delete_collections == True:
        db.drop_all_collections()
        print('The collections have been dropped!')

@app.command('skip')
def skip_endpoint(
    access_url: str = typer.Option(
        None,
        '--access-url',
        '-url',
        prompt='Endpoint access URL'
    )) -> None:
    """ Adds an empty endpoint to the database so that it could be skipper during the generation process """
    db.save_endpoint(
        {
            DB.ACCESS_URL: access_url,
            DB.STATUS: DB.STATUS_UNKNOWN
        }
    )
    print('The endpoint has been added to the databse and will be skipped on further queries')

@app.command('get-skipped')
def get_skipped() -> None:
    """ Adds an empty endpoint to the database so that it could be skipper during the generation process """
    endpoints = db.get_endpoint_collection({DB.STATUS : DB.STATUS_UNKNOWN})
    empty = True

    for endpoint in endpoints:
        empty = False
        print(endpoint[DB.ACCESS_URL])

    if empty:
        print('There are no skipped endpoints')

@app.command('dump-totals')
def get_endpoint_totals(output_file_name: str = typer.Option(
        config.get_file_config('endpoint_endpoints_totals'),
        '--output-file',
        '-o',
        prompt='Output dump file name')) -> None:
    """ Adds an empty endpoint to the database so that it could be skipper during the generation process """
    stats = db.get_endpoint_collection_totals()
    collection_dump.export_dump(output_file_name, stats)

@app.command('get-stats')
def get_stats(
        separate: bool = typer.Option(
        True,
        '--separate-domains',
        '-d',
        prompt='Separate by domains?'),
        output_file_name: str = typer.Option(
        config.get_file_config('endpoint_statistics'),
        '--output-file',
        '-o',
        prompt='Output dump file name')
) -> None:
    stats = db.get_statistics(separate)
    collection_dump.export_dump(output_file_name, stats)