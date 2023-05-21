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
    QUERY_EDITOR_NAME = 'query_editor_name'
    QUERY_EDITOR_ADDITIONAL_INFORMATION = 'query_editor_additional_information'
    TRIPLES_AMOUNT = 'triples_amount'
    CLASSES_AMOUNT = 'classes_amount'
    PROPERTIES_AMOUNT = 'properties_amount'
    USED_CLASSES = 'used_classes'
    USED_PROPERTIES = 'used_properties'
    ERROR_MESSAGE = 'error_message'

    CUSTOM_QUERIES = 'custom_queries'
    CUSTOM_QUERY_RESULT = 'custom_query_result'
    CUSTOM_QUERY_NAME = 'name'
    CUSTOM_QUERY_BODY = 'body'

    INSTANCE_NAME = 'name'
    INSTANCE_AMOUNT = 'amount'
    PROPERTY_ID = 'property_id'
    CLASS_ID = 'class_id'

    STATUS_OK = 'OK'
    STATUS_FAIL = 'FAIL'
    STATUS_UNKNOWN = 'UNKNOWN'
    STATUS_DUPLICATE = 'DUPLICATE'

    DUPLICATE_REFERENCE = 'duplicate_reference'

    def __init__(self):
        """ Sets up the connection with MongoDB """
        self.config = Config()
        self.client = MongoClient(
            host=self.config.get_db_config('host'), 
            port=int(self.config.get_db_config('port'))
        )

        self.db = self.client[self.config.get_db_config('name')]
        self.endpoints = self.db[self.config.get_db_config('endpoint_collection')]

    def save_endpoint(
            self,
            endpoint_data: Dict[str, Any]
        ) -> None:
        """ Saves a new endpoint in the endpoint collection """
        try:
            self.endpoints.insert_one(endpoint_data)
        except Exception as e:
            print(e)
 
    def update_endpoint(
            self,
            endpoint_data: Dict[str, Any]
        ):
        """ Updates existing endpoint with new data """

        if self.get_endpoint(endpoint_data[self.ACCESS_URL]):
            self.endpoints.update_one(
                { self.ACCESS_URL: endpoint_data[self.ACCESS_URL] },
                { '$set': endpoint_data }
            )

    def get_duplicate(
            self,
            access_url: str
        ):

        endpoint = self.get_endpoint(access_url)

        if endpoint == None:
            return None
        
        if endpoint[DB.TRIPLES_AMOUNT] == -1:
            return None

        result = self.endpoints.aggregate([
            {
                '$match': {
                    DB.TRIPLES_AMOUNT: endpoint[DB.TRIPLES_AMOUNT],
                    DB.CLASSES_AMOUNT: endpoint[DB.CLASSES_AMOUNT],
                    DB.USED_PROPERTIES: endpoint[DB.USED_PROPERTIES],
                    DB.USED_CLASSES: endpoint[DB.USED_CLASSES],
                    DB.CLASSES_AMOUNT: endpoint[DB.CLASSES_AMOUNT],
                    DB.ACCESS_URL: {
                        '$ne': access_url
                    } 
                }
            }
        ])

        if len(list(result)) > 0:
            return result

    def endpoint_has_custom_query(
            self,
            access_url,
            custom_query_name
        ):
        """ Chcecks whether the specified endpoint has the relevant query's field """
        endpoint = self.get_endpoint(access_url)

        if endpoint == None:
            return False

        return custom_query_name in self.get_endpoint(access_url)

    def get_endpoint(
            self, 
            access_url: str
        ):
        """ Returns an endpoint from the endpoint collection by access_url """
        return self.endpoints.find_one({'access_url': access_url})
    
    def delete_queries(
            self, 
            queries: list
        ):
        """ Deletes specified queries accross whole collection of endpoints """
        unset_fields = {}

        for query in queries:
            unset_fields[query] = 1

        return self.endpoints.update_many(
            {},
            {
                '$unset': unset_fields
            }
        )
    
    def drop_all_collections(self) -> None:
        """ Drops the whole endpoint collection alongisde with the database """
        self.endpoints.drop()

    def get_endpoint_collection(
            self, 
            filters: dict = {}
        ):
        """ Returns whole collection of endpoints """
        return self.endpoints.find(filters)

    def delete_endpoint(
            self,
            access_url
        ):

        return self.endpoints.delete_one({
            self.ACCESS_URL: access_url
        })
    
    def get_domains(self) -> None:
        return self.endpoints.aggregate([
            {
                '$unwind': {
                    'path': f'${self.DOMAIN}'
                },
            },
            {
                '$group': {
                    '_id': f'${self.DOMAIN}'
                }
            },
            {
                '$sort': {
                    '_id': -1
                }
            }
        ])
    
    def get_most_used_instances(
            self,
            instance_array_name: str,
            domain: str = None,
            limit: int = 50
        ):
        """ Retrievs the most used instances accross endpoints """
        pipeline = [
            {
                '$unwind': {
                    'path': f'${instance_array_name}'
                }
            },
            {
                '$group': {
                    '_id': {
                            self.INSTANCE_NAME: f'${instance_array_name}.{self.INSTANCE_NAME}',
                            self.DOMAIN: f'${self.DOMAIN}'
                        },
                    'name': {
                        '$first':  f'${instance_array_name}.{self.INSTANCE_NAME}'
                    },
                    'total': {
                        '$sum': f'${instance_array_name}.{self.INSTANCE_AMOUNT}'
                    },
                }
            },
            {
                '$unset': '_id'
            },
            {
                '$sort': {
                    'total': -1
                }
            },
            {
                '$limit': limit
            }
        ]

        if domain != None:
            pipeline.insert(0, {
                    '$match': {
                        self.DOMAIN: domain
                    }
                })

            pipeline.insert(0, {
                    '$unwind': {
                        'path': f'${self.DOMAIN}'
                    }
                })
                    
        return self.endpoints.aggregate(pipeline)
    
    def get_statistics(
            self, 
            separate=False
        ) -> Any:

        pipeline = [
            {
                '$group': {
                    '_id': '',
                    'endpoints_amount': {
                        '$sum': 1
                    },
                    "active_endpoints_amount": { 
                        "$sum": { 
                            "$switch": { 
                                "branches": [ 
                                    { 
                                        "case": { "$eq": [ "$status", f'{self.STATUS_OK}' ] }, 
                                        "then": 1
                                    }
                                ],
                                'default': 0
                            }
                        }
                    },
                    "failed_endpoints_amount": { 
                        "$sum": { 
                            "$switch": { 
                                "branches": [ 
                                    { 
                                        "case": { "$eq": [ "$status", f'{self.STATUS_FAIL}' ] }, 
                                        "then": 1
                                    }
                                ],
                                'default': 0
                            }
                        }
                    }, 
                    DB.TRIPLES_AMOUNT: {
                        '$sum': f'${DB.TRIPLES_AMOUNT}'
                    },
                    DB.CLASSES_AMOUNT: {
                        '$sum': f'${DB.CLASSES_AMOUNT}'
                    },
                    DB.PROPERTIES_AMOUNT: {
                        '$sum': f'${DB.PROPERTIES_AMOUNT}'
                    }
                },
            },
            {
                '$sort': {
                    '_id': 1
                }
            }
        ]

        if separate == True:
            pipeline[0]['$group']['_id'] = f'${self.DOMAIN}'

            pipeline.insert(0, {
                '$unwind': {
                    'path': f'${self.DOMAIN}'
                },
            })

        return self.endpoints.aggregate(pipeline)