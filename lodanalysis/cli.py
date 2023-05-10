from lodanalysis.config import Config
from lodanalysis.collection_dump import CollectionDump
from lodanalysis.lod_cloud import LODCloud
from lodanalysis.mongo_db import DB
from lodanalysis.sparql_data_extractor import SPARQLDataExtractor
import typer

app = typer.Typer()
db = DB()
config = Config()
collection_dump = CollectionDump()
lod_cloud = LODCloud()

@app.command()
def generate(
    output_file_name: str = typer.Option(
        config.get_file_config('collection_dump'),
        '--output-file',
        '-o'
    )
) -> None:
    """ Extracts data from the LOD Cloud JSON file and performs SPARQL queries on their endpoints """
    process_result = lod_cloud.process_data()
    if (process_result == False):
        print('An error has occured while trying to get the LOD Cloud JSON file')

        return
    else:
        print('The LOD Cloud processing has been finished!')

    collection_dump.export_endpoint_collection_dump(output_file_name)
    print('Data dump has been exported!')

@app.command()
def dump(
    output_file_name: str = typer.Option(
        config.get_file_config('collection_dump'),
        '--output-file',
        '-o',
        prompt='Output file'
    ),
    only_active: bool = typer.Option(
        False,
        confirmation_prompt=True,
        prompt='Include only active endpoints?'
    )
) -> None:
    """ Dumps the whole endpoint collection in JSON format """
    collection_dump.export_endpoint_collection_dump(output_file_name, only_active)
    print('Data dump has been created!')

@app.command()
def get(
    endpoint_access_url: str = typer.Option(
        None,
        '--access-url',
        '-url',
        prompt='SPARQL Endpoint Access URL'
    ),
    output_file_name: str = typer.Option(
        config.get_file_config('endpoint_dump'),
        '--output-file',
        '-o',
        prompt='Output file'
    ),
    save_endpoint: bool = typer.Option(
        None,
        confirmation_prompt=True,
        prompt='Save results in the database?'
    )
) -> None:
    """ Analyzes SPARQL endpoint by its URL and saved data to the database if required """
    data_extractor = SPARQLDataExtractor()
    endpoint_data = data_extractor.extract_data(endpoint_access_url)

    if (len(endpoint_data) > 0) & (save_endpoint == True):
        if db.get_endpoint(endpoint_access_url):
            db.update_endpoint(endpoint_data)
            print('The endpoint has been updated!')
        else:
            db.save_endpoint(endpoint_data)
            print('The endpoint has been saved!')

    collection_dump.export_dump(output_file_name, endpoint_data)

@app.command()
def download() -> None:
    """ Downloads the latest LOD cloud JSON file with raw datasets """
    input_file = config.get_file_config('raw_data') + '.json'

    lod_cloud.get_lod_cloud_json(input_file)

@app.command()
def drop(delete_endpoint: bool = typer.Option(
        None,
        confirmation_prompt=True,
        prompt='Are you sure you want to delete the endpoint collection?'
    )
):
    """ Drops the endpoint collection """
    if delete_endpoint == True:
        db.drop_endpoint_collection()
        print('The collection has been dropped!')