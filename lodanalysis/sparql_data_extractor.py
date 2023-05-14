from bs4 import BeautifulSoup
from lodanalysis.config import Config
from lodanalysis.mongo_db import DB
from lodanalysis.sparql_queries import SPARQLQueries
from typing import Dict, Any
import requests

class SPARQLDataExtractor:
    """ 
    Class for extracting data from SPARQL Endpoint 
    """
    def __init__(self):
        self.sparql_queries = SPARQLQueries()
        self.db = DB()
        self.config = Config()

    def __reset_local_endpoint(self) -> None:
        """ Sets/resets the local endpoint dictionary that's used for keeping data about SPARQL endpoint, triples, classes and properties """
        self.endpoint_data = {
            DB.ACCESS_URL: None
        }

    def extract_data(
            self,
            access_url: str,
            save_endpoint: bool = True,
            include_base_queries: bool = True,
            queries_directory:str = '',
            only_new_custom_queries: bool = True
        ) -> Dict[str, Any]:
        """ Makes SPARQL calls on the endpoint and fetches data """
        self.__reset_local_endpoint()
        self.save_endpoint = save_endpoint

        self.sparql_queries.set_wrapper(access_url)
        self.endpoint_data[DB.ACCESS_URL] = access_url

        if include_base_queries == True or only_new_custom_queries == False:
            inactive_endpoint = self.__test_connection()

            if inactive_endpoint != None:
                return inactive_endpoint

        if include_base_queries == True:
            self.__analyse()

        if queries_directory:
            self.__call_custom_queries(only_new_custom_queries, queries_directory)

        return self.endpoint_data
    
    def __test_connection(self):
        try:
            print('Testing connection...')
            self.sparql_queries.test_connection()
        except Exception as e:
            print(e)

            self.endpoint_data[DB.STATUS] = DB.STATUS_FAIL
            self.endpoint_data[DB.ERROR_MESSAGE] = str(e)

            return self.endpoint_data

        self.endpoint_data[DB.STATUS] = DB.STATUS_OK

    def __analyse(self):
        print('Getting editor...')
        editor_data = self.__get_query_editor()
        self.endpoint_data[DB.QUERY_EDITOR_NAME] = editor_data[DB.QUERY_EDITOR_NAME]
        self.endpoint_data[DB.QUERY_EDITOR_ADDITIONAL_INFORMATION] = editor_data[DB.QUERY_EDITOR_ADDITIONAL_INFORMATION]

        print('Getting total triples...')
        self.endpoint_data[DB.TRIPLES_AMOUNT] = self.sparql_queries.get_total_triple_amount()

        print('Getting total classes...')
        self.endpoint_data[DB.CLASSES_AMOUNT] = self.sparql_queries.get_total_class_amount()

        print('Getting most used classes...')
        self.endpoint_data[DB.MOST_USED_CLASSES] = self.__get_most_used_classes()

        print('Getting properties...')
        properties_data = self.__get_properties()
        self.endpoint_data[DB.PROPERTIES_AMOUNT] = properties_data[DB.PROPERTIES_AMOUNT]
        self.endpoint_data[DB.USED_PROPERTIES] = properties_data[DB.USED_PROPERTIES]
    
    def __get_query_editor(self) -> Dict[str, str]:
        """ Gets the SPARQL query editor infromation from the GET method """
        editor_data = {
            DB.QUERY_EDITOR_NAME: '',
            DB.QUERY_EDITOR_ADDITIONAL_INFORMATION: ''
        }

        try:
            request = requests.get(self.endpoint_data[DB.ACCESS_URL])
        except:
            return editor_data
        
        if request.ok:
            soup = BeautifulSoup(request.content, features='xml')
            title = soup.title

            if title:
                editor_data[DB.QUERY_EDITOR_NAME] = title.string

                if editor_data[DB.QUERY_EDITOR_NAME].lower().find('virtuoso') != -1:
                    editor_data[DB.QUERY_EDITOR_NAME] = 'Virtuoso'
                    footer = soup.find(id='footer')

                    if footer:
                        editor_data[DB.QUERY_EDITOR_ADDITIONAL_INFORMATION] = ' '.join(footer.text.split())

        return editor_data
    
    def __get_properties(self) -> Dict[str, Any]:
        """ Get all used properties from the endpoint with the total amount of them """
        properties = {
            DB.USED_PROPERTIES: [],
            DB.PROPERTIES_AMOUNT: None
        }
        used_propeties = self.sparql_queries.get_used_properties()
        are_properties_valid = used_propeties['is_valid']

        if are_properties_valid == True:
            for used_property in used_propeties['value']:
                used_property_name = used_property[self.sparql_queries.PROPERTY]['value']
                used_property_amount = used_property[self.sparql_queries.PROPERTY_AMOUNT]['value']

                if self.save_endpoint == True:
                    property_id = self.db.save_property_if_not_exists(used_property_name)
                    properties[DB.USED_PROPERTIES].append({
                        DB.PROPERTY_ID: property_id, 
                        DB.INSTANCE_AMOUNT: int(used_property_amount)
                    })
                else:
                    properties[DB.USED_PROPERTIES].append({
                        DB.INSTANCE_NAME: used_property_name, 
                        DB.INSTANCE_AMOUNT: int(used_property_amount)
                    })
                
        properties[DB.PROPERTIES_AMOUNT] = len(used_propeties['value']) if are_properties_valid else -1

        return properties

    def __get_most_used_classes(self) -> list:
        """ Get the most used classes from the endpoint """
        classes = []

        for used_class in self.sparql_queries.get_most_used_classes():
            used_class_name = used_class[self.sparql_queries.CLASS]['value']
            used_class_amount = used_class[self.sparql_queries.CLASS_AMOUNT]['value']

            if self.save_endpoint == True:
                class_id = self.db.save_class_if_not_exists(used_class_name)
                classes.append({
                    DB.CLASS_ID: class_id, 
                    DB.INSTANCE_AMOUNT: int(used_class_amount)
                })
            else:
                classes.append({
                    DB.INSTANCE_NAME: used_class_name, 
                    DB.INSTANCE_AMOUNT: int(used_class_amount)
                })

        return classes
    
    def __call_custom_queries(
            self,
            only_new: bool,
            queries_directory_name: str
        ):
        print('Performing custom queries...')
        custom_queries = self.config.get_custom_queries(queries_directory_name)

        for query in custom_queries:
            if only_new == True and self.db.endpoint_has_custom_query(self.endpoint_data[DB.ACCESS_URL], query[DB.CUSTOM_QUERY_NAME]):
                continue
            
            query_result = self.sparql_queries.get_custom_query_result(query[DB.CUSTOM_QUERY_BODY])
            self.endpoint_data[query[DB.CUSTOM_QUERY_NAME]] = query_result