from bs4 import BeautifulSoup
from lodanalysis.mongo_db import DB
from lodanalysis.sparql_queries import SPARQLQueries
import requests
from typing import Dict, Any

class SPARQLDataExtractor:
    """ Extracting data from SPARQL Endpoint """
    def __init__(self):
        self.sparql_queries = SPARQLQueries()
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

        request = requests.get(access_url)

        # Get query Editor name if there is a response on GET method
        if request.status_code == 200:
            soup = BeautifulSoup(request.content, features='xml')
            title = soup.title

            if title: 
                self.endpoint_data[DB.QUERY_EDITOR_NAME] = title.string

                if self.endpoint_data[DB.QUERY_EDITOR_NAME].lower().find('virtuoso') != -1:
                    self.endpoint_data[DB.QUERY_EDITOR_NAME] = 'Virtuoso'
                    footer = soup.find(id='footer')

                    if footer:
                        self.endpoint_data[DB.QUERY_EDITOR_ADDITIONAL_INFORMATION] = ' '.join(footer.text.split())

        self.endpoint_data[DB.TRIPLES_AMOUNT] = self.sparql_queries.get_total_triple_amount()
        self.endpoint_data[DB.CLASSES_AMOUNT] = self.sparql_queries.get_total_class_amount()

        used_propeties = self.sparql_queries.get_most_used_properties()
        are_properties_valid = used_propeties['is_valid']
        if are_properties_valid == True:
            for used_property in used_propeties['value']:
                used_property_name = used_property[self.sparql_queries.PROPERTY]['value']
                used_property_amount = used_property[self.sparql_queries.PROPERTY_AMOUNT]['value']
                self.endpoint_data[DB.USED_PROPERTIES].append({'name': used_property_name, 'amount': used_property_amount})
                
        self.endpoint_data[DB.PROPERTIES_AMOUNT] = len(used_propeties['value']) if are_properties_valid else -1

        for used_class in self.sparql_queries.get_most_used_classes():
            used_class_name = used_class[self.sparql_queries.CLASS]['value']
            used_class_amount = used_class[self.sparql_queries.CLASS_AMOUNT]['value']
            self.endpoint_data[DB.MOST_USED_CLASSES].append({'name': used_class_name, 'amount': used_class_amount})

        return self.endpoint_data