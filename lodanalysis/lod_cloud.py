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
            void_access_url = ''

            for download in other_downloads:
                if (('title' in download) and (bool(download['title'])) and ('void' in download['title'].lower())) or (('description' in download) and (bool(download['description'])) and ('void' in download['description'])):
                    void_access_url = download['access_url']

            for download in other_downloads:
                if (('title' in download) and (bool(download['title'])) and ('sparql' in download['title']) or (('description' in download) and (bool(download['description'])) and ('sparql' in download['description']))):
                    self.__set_endpoint_data(download, include_base_queries, queries_directory, dataset_code, void_access_url, is_sparql=False)

            for endpoint in endpoints:
                self.__set_endpoint_data(endpoint, include_base_queries, queries_directory, dataset_code, void_access_url)

        file.close()

        return True
    
    def __get_str_value(self, arr: list, key: str) -> str:
        return arr[key] if (key in arr) and bool(arr[key]) else ''
    
    def __set_endpoint_data(
            self,
            endpoint: str,
            include_base_queries: bool, 
            queries_directory: str,
            dataset_code: str,
            void_access_url: str,
            is_sparql: bool = True
        ) -> None:
        access_url = endpoint[DB.ACCESS_URL]
        print(access_url)

        existing_endpoint = self.db.get_endpoint(access_url)

        endpoint_title = self.__get_str_value(endpoint, 'title')
        endpoint_description = self.__get_str_value(endpoint, 'description')
        dataset_title = self.__get_str_value(self.dataset_data, 'title')
        dataset_description = self.__get_str_value(self.dataset_data, 'description')
        dataset_description_en = self.__get_str_value(dataset_description, 'en')
        total_description = {}

        if endpoint_title:
            total_description[DB.ENDPOINT_TITLE] = endpoint_title

        if endpoint_description:
            total_description[DB.ENDPOINT_DESCRIPTION] = endpoint_description

        if dataset_title:
            total_description[DB.DATASET_TITLE] = dataset_title

        if dataset_description:
            total_description[DB.DATASET_DESCRIPTION] = dataset_description_en

        total_description[DB.DATASET_CODE] = dataset_code

        if (self.db.get_endpoint(access_url) != None):
            if existing_endpoint[DB.STATUS] == DB.STATUS_OK and 'domain' in self.dataset_data:
                domain = self.dataset_data['domain']
                domains = existing_endpoint[DB.DOMAINS]
                
                if domain not in domains:
                    domains.append(domain)
                    existing_endpoint[DB.DOMAINS] = domains
                    total_description[DB.DOMAIN] = domain
                    
                    names = existing_endpoint[DB.NAMES]
                    print(names)
                    names.append(total_description)
                    existing_endpoint[DB.NAMES] = names
                    if is_sparql:
                        existing_endpoint[DB.SPARQL] = True
                    else:
                        existing_endpoint[DB.OTHER_DOWNLOAD] = True
                    
                    self.db.update_endpoint(existing_endpoint)
            return

        extracted_endpoint_data = self.data_extractor.extract_data(
            access_url,
            include_base_queries=include_base_queries,
            queries_directory=queries_directory,
            only_new_custom_queries=False
        )
        if void_access_url:
            extracted_endpoint_data[DB.VOID_ACCESS_URL] = void_access_url

        if is_sparql:
            extracted_endpoint_data[DB.SPARQL] = True
        else:
            extracted_endpoint_data[DB.OTHER_DOWNLOAD] = True

        extracted_endpoint_data[DB.DOMAINS] = [self.dataset_data['domain']] if 'domain' in self.dataset_data else []

        total_description[DB.DOMAIN] = extracted_endpoint_data[DB.DOMAINS][0]

        extracted_endpoint_data[DB.NAMES] = [total_description]

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