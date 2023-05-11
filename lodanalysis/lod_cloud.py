from lodanalysis.config import Config
from lodanalysis.mongo_db import DB
from lodanalysis.sparql_queries import SPARQLQueries
from lodanalysis.sparql_data_extractor import SPARQLDataExtractor
import json
import os
import urllib.request

class LODCloud:
    """ 
    Class for parsing the LOD Cloud JSON data 
    """
    def __init__(self):
        self.db = DB()
        self.sparql_queries = SPARQLQueries()
        self.config = Config()
        self.data_extractor = SPARQLDataExtractor()

    def process_data(self) -> bool:
        """ Reads the file, extracts data from the datasets, makes SPARQL query calls and saves data """
        input_file = self.config.get_file_config('raw_data') + '.json'

        if os.path.exists(input_file) == False:
            if self.get_lod_cloud_json(input_file) == False:
                return False

        file = open(input_file)
        file_data = json.load(file)

        # Process each dataset sequentially
        for dataset_code in file_data:
            dataset_data = file_data[dataset_code]
            endpoints = dataset_data['sparql']

            if (len(endpoints) < 1):
                continue

            for endpoint in endpoints:
                access_url = endpoint['access_url']
                print(access_url)

                if (self.db.get_endpoint(access_url) != None):
                    continue

                extracted_endpoint_data = self.data_extractor.extract_data(access_url)
                name = endpoint['title'] if ('title' in endpoint) and bool(endpoint['title']) else dataset_data['title']
                domain = dataset_data['domain'] if 'domain' in dataset_data else None
                self.db.save_endpoint(extracted_endpoint_data, name, domain)

        file.close()

        return True
    
    def get_lod_cloud_json(self, file_name: str) -> bool:
        """ Downloads the latest raw data JSON from the LOD Cloud """
        json_url = self.config.get_lod_cloud_config('latest_json_url')

        try:
            with urllib.request.urlopen(json_url) as url:
                data = url.read().decode('utf-8')
        except Exception:
            return False
        
        with open(file_name, 'w') as f:
            f.write(data)

        return True