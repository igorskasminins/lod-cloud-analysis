from pymongo import MongoClient
from lodanalysis.config import Config
from typing import Dict, Any

class DB:
    """ 
    Wrapper class for MongoDB queries 
    """
    ACCESS_URL = 'access_url'
    NAME = 'name'
    DOMAIN = 'domain'
    STATUS = 'status'
    ERROR_MESSAGE = 'error_message'
    QUERY_EDITOR_NAME = 'query_editor_name'
    QUERY_EDITOR_ADDITIONAL_INFORMATION = 'query_editor_additional_information'
    TRIPLES_AMOUNT = 'triples_amount'
    CLASSES_AMOUNT = 'classes_amount'
    PROPERTIES_AMOUNT = 'properties_amount'
    USED_PROPERTIES = 'used_properties'
    MOST_USED_CLASSES = 'most_used_classes'

    PROPERTY_ID = 'property_id'
    CLASS_ID = 'class_id'

    STATUS_OK = 'OK'
    STATUS_FAIL = 'FAIL'

    def __init__(self):
        """ Sets up connection with MongoDB """
        self.config = Config()
        self.client = MongoClient(
            host=self.config.get_db_config('host'), 
            port=int(self.config.get_db_config('port'))
        )

        self.db = self.client[self.config.get_db_config('name')]
        self.endpoints = self.db[self.config.get_db_config('endpoint_collection')]
        self.properties = self.db[self.config.get_db_config('property_collection')]
        self.classes = self.db[self.config.get_db_config('class_collection')]

    def save_endpoint(
            self,
            endpoint_data: Dict[str, Any],
            name: str = '',
            domain: str = ''
        ) -> None:
        """ Saves a new endpoint in the endpoint collection """
        endpoint = {
            self.ACCESS_URL: endpoint_data[self.ACCESS_URL],
            self.STATUS: endpoint_data[self.STATUS],
            self.NAME: name,
            self.DOMAIN: domain,
            self.QUERY_EDITOR_NAME: endpoint_data[self.QUERY_EDITOR_NAME],
            self.QUERY_EDITOR_ADDITIONAL_INFORMATION: endpoint_data[self.QUERY_EDITOR_ADDITIONAL_INFORMATION],
            self.TRIPLES_AMOUNT: endpoint_data[self.TRIPLES_AMOUNT],
            self.PROPERTIES_AMOUNT: endpoint_data[self.PROPERTIES_AMOUNT],
            self.CLASSES_AMOUNT: endpoint_data[self.CLASSES_AMOUNT],
            self.USED_PROPERTIES: endpoint_data[self.USED_PROPERTIES],
            self.MOST_USED_CLASSES: endpoint_data[self.MOST_USED_CLASSES],
            self.ERROR_MESSAGE: endpoint_data[self.ERROR_MESSAGE]
        }
        try:
            self.endpoints.insert_one(endpoint)
        except Exception as e:
            print(e)
        
    def update_endpoint(
            self,
            endpoint_data: Dict[str, Any]
        ):
        """ Updates existing endpoint with new data """
        access_url = endpoint_data[self.ACCESS_URL]

        if self.get_endpoint(endpoint_data[self.ACCESS_URL]):
            self.endpoints.update_one(
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
                        self.MOST_USED_CLASSES: endpoint_data[self.MOST_USED_CLASSES],
                        self.ERROR_MESSAGE: endpoint_data[self.ERROR_MESSAGE],
                    }
                }
            )

    def get_endpoint(
            self, 
            access_url: str
        ):
        """ Returns an endpoint from the endpoint collection by access_url """
        return self.endpoints.find_one({'access_url': access_url}, {})
    
    def drop_all_collections(self) -> None:
        """ Drops the whole endpoint collection alongisde with the database """
        self.endpoints.drop()
        self.properties.drop()
        self.classes.drop()

    def get_endpoint_collection(
            self, 
            only_active: bool
        ):
        """ Returns whole collection of endpoints """
        filters = {}

        if only_active == True:
            filters['status'] = self.STATUS_OK

        return self.endpoints.find(filters)
    
    def __get_property(
            self, 
            name: str
        ):
        """ Retrieves a property by the name """
        return self.properties.find_one({'name': name}, {})
    
    def save_property_if_not_exists(
            self, 
            name: str
        ):
        """ Saves the property in the property collection if does not exists yet and return the id of the property """
        existing_proeprty = self.__get_property(name)
        if existing_proeprty:
            return existing_proeprty['_id']
            
        inserted_property = self.properties.insert_one({
            'name': name
        })

        return inserted_property.inserted_id
    
    def __get_class(
            self, 
            name: str
        ):
        """ Retrieves a class by the name """

        return self.classes.find_one({'name': name}, {})

    def save_class_if_not_exists(
            self, 
            name: str
        ):
        """ Saves the class in the class collection if does not exists yet and return the id if the class """
        existing_class = self.__get_class(name)
        if existing_class:
            return existing_class['_id']
        
        inserted_class = self.classes.insert_one({
            'name': name
        })

        return inserted_class.inserted_id
    
    def get_most_used_instances(
            self, 
            instance_array_name: str, 
            instance_name: str
        ):
        """ Retrievs the most used instances accross all endpoints """
        pipeline = [
            {
                '$unwind': {
                    'path': f'${instance_array_name}'
                }
            },
            {
                '$group': {
                    '_id': f'${instance_array_name}.{instance_name}_id',
                      'total': {
                        '$sum': f'${instance_array_name}.amount'
                    }
                }
            },
            {
                '$lookup': {
                    'from': f'{instance_name}',
                    'localField': '_id',
                    'foreignField': '_id',
                    'as': f'{instance_name}'
                }
            },
            {
                '$set': {
                    'name': {
                        '$arrayElemAt': [f'${instance_name}.name', 0]
                    }
                }
            },
            {
                '$unset': f'{instance_name}'
            },
            {
                '$sort': {
                    'total': -1
                }
            },
            {
                '$limit': 50
            }
        ]

        return self.endpoints.aggregate(pipeline)