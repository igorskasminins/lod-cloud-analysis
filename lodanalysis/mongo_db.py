from pymongo import MongoClient
from lodanalysis.config import Config

# Wrapper class for MongoDB queries
class DB:
    ACCESS_URL = 'access_url'
    NAME = 'name'
    STATUS = 'status'
    ERROR_MESSAGE = 'error_message'
    QUERY_EDITOR_NAME = 'query_editor_name'
    QUERY_EDITOR_ADDITIONAL_INFORMATION = 'query_editor_additional_information'
    TRIPLES_AMOUNT = 'triples_amount'
    CLASSES_AMOUNT = 'classes_amount'
    PROPERTIES_AMOUNT = 'properties_amount'
    USED_PROPERTIES = 'used_propeties'
    USED_CLASSES = 'used_classes'

    STATUS_OK = 'OK'
    STATUS_FAIL = 'FAIL'

    # Sets up connection with database
    def __init__(self):
        self.config = Config()
        self.client = MongoClient(
            host=self.config.get_db_config('host'), 
            port=int(self.config.get_db_config('port'))
        )

        self.db = self.client[self.config.get_db_config('name')]
        self.endpoint = self.db.endpoint

    # Saves a new endpoint in the collection
    def save_endpoint(
            self,
            endpoint_data: dict,
            name: str = ''
            ):
        endpoint = {
            self.ACCESS_URL: endpoint_data[self.ACCESS_URL],
            self.STATUS: endpoint_data[self.STATUS],
            self.NAME: name,
            self.QUERY_EDITOR_NAME: endpoint_data[self.QUERY_EDITOR_NAME],
            self.QUERY_EDITOR_ADDITIONAL_INFORMATION: endpoint_data[self.QUERY_EDITOR_ADDITIONAL_INFORMATION],
            self.TRIPLES_AMOUNT: endpoint_data[self.TRIPLES_AMOUNT],
            self.PROPERTIES_AMOUNT: endpoint_data[self.PROPERTIES_AMOUNT],
            self.CLASSES_AMOUNT: endpoint_data[self.CLASSES_AMOUNT],
            self.USED_PROPERTIES: endpoint_data[self.USED_PROPERTIES],
            self.USED_CLASSES: endpoint_data[self.USED_CLASSES],
            self.ERROR_MESSAGE: endpoint_data[self.ERROR_MESSAGE]
        }

        self.endpoint.insert_one(endpoint)

    # Updates existing endpoint with new data
    def update_endpoint(self, endpoint_data: dict):
        access_url = endpoint_data[self.ACCESS_URL]

        if self.get_endpoint(endpoint_data[self.ACCESS_URL]):
            self.endpoint.update_one(
                { self.ACCESS_URL: access_url },
                {
                    '$set': {
                        self.STATUS: endpoint_data[self.STATUS],
                        self.QUERY_EDITOR_NAME: endpoint_data[self.QUERY_EDITOR_NAME],
                        self.QUERY_EDITOR_ADDITIONAL_INFORMATION: endpoint_data[self.QUERY_EDITOR_ADDITIONAL_INFORMATION],
                        self.TRIPLES_AMOUNT: endpoint_data[self.TRIPLES_AMOUNT],
                        self.PROPERTIES_AMOUNT: endpoint_data[self.PROPERTIES_AMOUNT],
                        self.CLASSES_AMOUNT: endpoint_data[self.CLASSES_AMOUNT],
                        self.USED_PROPERTIES: endpoint_data[self.USED_PROPERTIES],
                        self.USED_CLASSES: endpoint_data[self.USED_CLASSES],
                        self.ERROR_MESSAGE: endpoint_data[self.ERROR_MESSAGE],
                    }
                }
            )

    # Returns an endpoint from the endpoint collection by access_url
    def get_endpoint(self, access_url: str):
        return self.endpoint.find_one({'access_url': access_url}, {})
    
    # Drops the whole endpoint collection alongisde with the database
    def drop_endpoint_collection(self):
        return self.endpoint.drop()

    # Returns whole collection of endpoints
    def get_endpoint_collection(self):
        return self.endpoint.find({})
    
    # Returns collection of active endpoints
    def get_endpoint_collection(self):
        return self.endpoint.find({'status': self.STATUS_OK})