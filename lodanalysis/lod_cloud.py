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

    def process_data(
            self,
            include_base_queries=True,
            queries_directory=None
        ) -> bool:
        """ Reads the file, extracts data from the datasets, makes SPARQL query calls and saves data """
        input_file = self.config.get_file_config('raw_data') + '.json'

        if os.path.exists(input_file) == False:
            if self.get_lod_cloud_json(input_file) == False:
                return False

        file = open(input_file)
        file_data = json.load(file)

        # Process each dataset sequentially
        for dataset_code in file_data:
            self.dataset_data = file_data[dataset_code]
            endpoints = self.dataset_data['sparql']
            other_downloads = self.dataset_data['other_download']
            
            for download in other_downloads:
                if ('description' in download) and ('sparql' in download['description']):
                    self.__set_endpoint_data(download, include_base_queries, queries_directory)

            for endpoint in endpoints:
                self.__set_endpoint_data(endpoint, include_base_queries, queries_directory)

        file.close()

        return True

    def __set_endpoint_data(
            self, 
            endpoint: str,
            include_base_queries: bool, 
            queries_directory: str
        ) -> None:
        access_url = endpoint[DB.ACCESS_URL]
        print(access_url)

        existing_endpoint = self.db.get_endpoint(access_url)

        if (self.db.get_endpoint(access_url) != None):
            if existing_endpoint[DB.STATUS] == DB.STATUS_OK and 'domain' in self.dataset_data:
                domain = self.dataset_data['domain']
                domains = existing_endpoint[DB.DOMAIN]
                if domain not in domains:
                    domains.append(domain)
                    existing_endpoint[DB.DOMAIN] = domains
                    self.db.update_endpoint(existing_endpoint)                
            return

        extracted_endpoint_data = self.data_extractor.extract_data(
            access_url,
            include_base_queries=include_base_queries,
            queries_directory=queries_directory,
            only_new_custom_queries=False
        )

        extracted_endpoint_data[DB.NAME] = endpoint['title'] if ('title' in endpoint) and bool(endpoint['title']) else self.dataset_data['title']
        extracted_endpoint_data[DB.DOMAIN] = [self.dataset_data['domain']] if 'domain' in self.dataset_data else []

        duplicate = self.db.get_duplicate(access_url)
        if duplicate != None:
            extracted_endpoint_data[DB.STATUS] = DB.STATUS_DUPLICATE
            extracted_endpoint_data[DB.DUPLICATE_REFERENCE] = duplicate[DB.ACCESS_URL]

        self.db.save_endpoint(extracted_endpoint_data)
        
    def get_lod_cloud_json(
            self, 
            file_name: str
        ) -> bool:
        """ Downloads the latest raw data JSON from the LOD Cloud """
        json_url = self.config.get_lod_cloud_config('latest_json_url')
        data = ''

        try:
            with urllib.request.urlopen(json_url) as url:
                data = url.read().decode('utf-8')
        except Exception:
            return False

        with open(file_name, 'w') as f:
            f.write(data)

        return True