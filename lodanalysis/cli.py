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
    output_file_name: str = typer.Option(
        config.get_file_config('collection_dump'),
        '--output-file',
        '-o'
    ),
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
        print('The specified directory does not exist or empty')
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

    active_endpoints = db.get_endpoint_collection(only_active=True)
    for endpoint in active_endpoints:
        updated_endpoint = data_extractor.extract_data(
            endpoint[DB.ACCESS_URL],
            include_base_queries=False,
            queries_directory=queries_directory,
            only_new_custom_queries=only_new_custom_queries,
        )

        db.update_endpoint(updated_endpoint)
        
@app.command()
def get(
    endpoint_access_url: str = typer.Option(
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
        access_url=endpoint_access_url,
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
    collection_dump.export_endpoint_collection_dump(output_file_name, only_active)
    print('Data dump has been created!')

@app.command('top-properties')
def top_properties(
    output_file_name: str = typer.Option(
        config.get_file_config('top_properties_dump'),
        '--output-file',
        '-o',
        prompt='Output dump file name'
    )
) -> None:
    """ Retrieves the most used properties accross all endpoints """
    properties = db.get_most_used_instances(DB.USED_PROPERTIES, SPARQLQueries.PROPERTY)
    collection_dump.export_dump(output_file_name, properties)

@app.command('top-classes')
def top_classes(
        output_file_name: str = typer.Option(
        config.get_file_config('top_classes_dump'),
        '--output-file',
        '-o',
        prompt='Output dump file name'
    )
) -> None:
    """ Retrieves the most used classes accross all endpoints """
    classes = db.get_most_used_instances(DB.MOST_USED_CLASSES, SPARQLQueries.CLASS)
    collection_dump.export_dump(output_file_name, classes)

@app.command('delete-query')
def delete_query(
    query_name: str = typer.Option(
        None,
        '--output-file',
        '-o',
        prompt='Query name'
    )
) -> None:
    """ Delete results from a single query accross all endpoints """
    db.delete_query([query_name])

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

@app.command('delete-endpoint')
def delete_endpoint(
    access_url: str = typer.Option(
        None,
        '--access-url',
        '-url',
        prompt='Endpoint access URL'
    )) -> None:

    db.delete_endpoint(access_url)

@app.command()
def drop(delete_collections: bool = typer.Option(
        True,
        confirmation_prompt=True,
        prompt='Are you sure you want to delete all collections?'
    )
) -> None:
    """ Drops the endpoint, property and class collection """
    if delete_collections == True:
        db.drop_all_collections()
        print('The collections have been dropped!')

@app.command()
def skip(
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