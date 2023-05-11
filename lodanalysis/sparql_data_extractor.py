from bs4 import BeautifulSoup
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

    def __reset_local_endpoint(self) -> None:
        """ Sets/resets the local endpoint dictionary that's used for keeping data about SPARQL endpoint, classes and properties """
        self.endpoint_data = {
            DB.ACCESS_URL: None,
            DB.STATUS: None,
            DB.QUERY_EDITOR_NAME: None,
            DB.QUERY_EDITOR_ADDITIONAL_INFORMATION: None,
            DB.TRIPLES_AMOUNT: None,
            DB.PROPERTIES_AMOUNT: None,
            DB.CLASSES_AMOUNT: None,
            DB.USED_PROPERTIES: [],
            DB.MOST_USED_CLASSES: [],
            DB.ERROR_MESSAGE: None
        }

    def extract_data(
            self,
            access_url: str
        ) -> Dict[str, Any]:
        """ Makes SPARQL calls on the endpoint and fetches data """
        self.__reset_local_endpoint()

        self.sparql_queries.set_wrapper(access_url)
        self.endpoint_data[DB.ACCESS_URL] = access_url

        try:
            self.sparql_queries.test_connection()
        except Exception as e:
            print(e)

            self.endpoint_data[DB.STATUS] = DB.STATUS_FAIL
            self.endpoint_data[DB.ERROR_MESSAGE] = str(e)

            return self.endpoint_data

        self.endpoint_data[DB.STATUS] = DB.STATUS_OK

        editor_data = self.__get_query_editor(access_url)
        self.endpoint_data[DB.QUERY_EDITOR_NAME] = editor_data[DB.QUERY_EDITOR_NAME]
        self.endpoint_data[DB.QUERY_EDITOR_ADDITIONAL_INFORMATION] = editor_data[DB.QUERY_EDITOR_ADDITIONAL_INFORMATION]

        self.endpoint_data[DB.TRIPLES_AMOUNT] = self.sparql_queries.get_total_triple_amount()
        self.endpoint_data[DB.CLASSES_AMOUNT] = self.sparql_queries.get_total_class_amount()
        self.endpoint_data[DB.MOST_USED_CLASSES] = self.__get_most_used_classes()

        properties_data = self.__get_properties()
        self.endpoint_data[DB.PROPERTIES_AMOUNT] = properties_data[DB.PROPERTIES_AMOUNT]
        self.endpoint_data[DB.USED_PROPERTIES] = properties_data[DB.USED_PROPERTIES]

        return self.endpoint_data
    
    def __get_query_editor(
            self, 
            access_url: str
        ) -> Dict[str, str]:
        """ Gets the SPARQL query editor infromation from the GET method """
        request = requests.get(access_url)
        editor_data = {
            DB.QUERY_EDITOR_NAME: '',
            DB.QUERY_EDITOR_ADDITIONAL_INFORMATION: ''
        }

        if request.status_code == 200:
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
                property_id = self.db.save_property_if_not_exists(used_property_name)

                used_property_amount = used_property[self.sparql_queries.PROPERTY_AMOUNT]['value']
                properties[DB.USED_PROPERTIES].append({DB.PROPERTY_ID: property_id, 'amount': int(used_property_amount)})
                
        properties[DB.PROPERTIES_AMOUNT] = len(used_propeties['value']) if are_properties_valid else -1

        return properties

    def __get_most_used_classes(self) -> list:
        """ Get the most used classes from the endpoint """
        classes = []

        for used_class in self.sparql_queries.get_most_used_classes():
            used_class_name = used_class[self.sparql_queries.CLASS]['value']
            class_id = self.db.save_class_if_not_exists(used_class_name)

            used_class_amount = used_class[self.sparql_queries.CLASS_AMOUNT]['value']
            classes.append({DB.CLASS_ID: class_id, 'amount': int(used_class_amount)})
        
        return classes