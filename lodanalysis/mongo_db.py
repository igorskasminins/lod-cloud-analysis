from pymongo import MongoClient
from lodanalysis.config import Config
from pymongo.cursor import Cursor
from pymongo.results import UpdateResult, DeleteResult
from typing import Dict, Any

class DB:
    """ 
    Wrapper class for MongoDB queries 
    """
    ACCESS_URL = 'access_url'
    NAMES = 'names'
    DOMAINS = 'domains'
    DOMAIN = 'domain'
    STATUS = 'status'
    QUERY_EDITOR_NAME = 'query_editor_name'
    QUERY_EDITOR_ADDITIONAL_INFORMATION = 'query_editor_additional_information'
    TRIPLES_AMOUNT = 'triples_amount'
    CLASSES_AMOUNT = 'used_classes_amount'
    UNIQUE_SUBJECTS_AMOUNT = 'unique_subjects_amount'
    AVERAGE_UNIQUE_SUBJECTS_AMOUNT = 'average_unique_subjects_amount'
    INSTANCES_AMOUNT = 'instances_amount'
    USED_PROPERTIES_AMOUNT = 'used_properties_amount'
    PROPERTIES_AMOUNT = 'properties_amount'
    USED_CLASSES = 'used_classes'
    USED_PROPERTIES = 'used_properties'
    VOID_ACCESS_URL = 'void_access_url'
    ERROR_MESSAGE = 'error_message'

    ENDPOINT_TITLE = 'endpoint_title'
    ENDPOINT_DESCRIPTION = 'endpoint_description'
    DATASET_NAME = 'dataset_name'
    DATASET_TITLE = 'dataset_title'
    DATASET_DESCRIPTION = 'dataset_description'
    DATASET_CODE = 'dataset_code'

    SPARQL = 'sparql'
    OTHER_DOWNLOAD = 'sparql'

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
        ) -> Any:
        """ Updates existing endpoint with new data """

        if self.get_endpoint(endpoint_data[self.ACCESS_URL]):
            return self.endpoints.update_one(
                { self.ACCESS_URL: endpoint_data[self.ACCESS_URL] },
                { '$set': endpoint_data }
            )


    def get_duplicate(
            self,
            access_url: str
        ) -> Any:
        """ Check whether the results correspond to an existing record endpoint record and return the original record  """
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
            access_url: str,
            custom_query_name: str
        ) -> bool:
        """ Chcecks whether the specified endpoint has the relevant query's field """
        endpoint = self.get_endpoint(access_url)

        if endpoint == None:
            return False

        return custom_query_name in self.get_endpoint(access_url)

    def get_endpoint(
            self, 
            access_url: str
        ) -> Any:
        """ Returns an endpoint from the endpoint collection by access_url """
        return self.endpoints.find_one({'access_url': access_url})

    def delete_queries(
            self, 
            queries: list
        ) -> UpdateResult:
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
    ) -> Cursor:
        """ Returns whole collection of endpoints """
        return self.endpoints.find(filters)

    def get_endpoint_collection_totals(self, domain=None) -> Cursor:
        """ Returns whole collection of endpoints """
        conditions = {self.STATUS: self.STATUS_OK}

        if domain != None:
            conditions[self.DOMAINS] = {
                '$in': [domain]
            }

        return self.endpoints.find(conditions, {
            self.ACCESS_URL:1, 
            self.TRIPLES_AMOUNT: 1, 
            self.CLASSES_AMOUNT: 1, 
            self.INSTANCES_AMOUNT: 1, 
            self.USED_PROPERTIES_AMOUNT: 1,
            self.UNIQUE_SUBJECTS_AMOUNT: 1,
            self.AVERAGE_UNIQUE_SUBJECTS_AMOUNT: 1,
            self.TRIPLES_AMOUNT:1, 
            '_id': 0}).sort([
                [self.TRIPLES_AMOUNT, -1], 
                [self.CLASSES_AMOUNT, -1], 
                [self.INSTANCES_AMOUNT, -1]
            ])

    def delete_endpoint(
            self,
            access_url: str
        ):
        """ Deletes an existing endpoint by the access_url """
        return self.endpoints.delete_one({
            self.ACCESS_URL: access_url
        })
    
    def get_domains(self) -> None:
        """ Retrievs the most used instances accross endpoints """
        return self.endpoints.aggregate([
            {
                '$unwind': {
                    'path': f'${self.DOMAINS}'
                },
            },
            {
                '$group': {
                    '_id': f'${self.DOMAINS}'
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
        ) -> Any:
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
                            self.INSTANCE_NAME: f'${instance_array_name}.{self.INSTANCE_NAME}'
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
                        self.DOMAINS: domain
                    }
                })

            pipeline.insert(0, {
                    '$unwind': {
                        'path': f'${self.DOMAINS}'
                    }
                })
                    
        return self.endpoints.aggregate(pipeline)
    
    def get_statistics(
            self, 
            separate: bool = False
        ) -> Any:
        """ Get statistics across all amounts from the collection """
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
                    "unknown_endpoints_amount": { 
                        "$sum": { 
                            "$switch": { 
                                "branches": [ 
                                    { 
                                        "case": { "$eq": [ "$status", f'{self.STATUS_UNKNOWN}' ] }, 
                                        "then": 1
                                    }
                                ],
                                'default': 0
                            }
                        }
                    },
                    "duplicate_endpoints_amount": { 
                        "$sum": { 
                            "$switch": { 
                                "branches": [ 
                                    { 
                                        "case": { "$eq": [ "$status", f'{self.STATUS_DUPLICATE}' ] }, 
                                        "then": 1
                                    }
                                ],
                                'default': 0
                            }
                        }
                    },
                    "can_get_triple_amount": { 
                        "$sum": { 
                            "$switch": { 
                                "branches": [ 
                                    { 
                                        "case": { 
                                            '$and': [
                                                {"$gt": [ f"${self.TRIPLES_AMOUNT}", 0 ]},
                                                {'$ne': [ f"${self.TRIPLES_AMOUNT}", 10000 ]},
                                                {'$ne': [ f"${self.TRIPLES_AMOUNT}", 100000 ]},
                                                {'$eq': [ f'${self.STATUS}', self.STATUS_OK]}

                                            ]
                                        }, 
                                        "then": 1
                                    }
                                ],
                                'default': 0
                            }
                        }
                    },
                    DB.TRIPLES_AMOUNT: {
                        "$sum": { 
                            "$switch": { 
                                "branches": [ 
                                    { 
                                        "case": { 
                                            '$and': [
                                                {"$gt": [ f"${self.TRIPLES_AMOUNT}", 0 ]},
                                                {'$ne': [ f"${self.TRIPLES_AMOUNT}", 10000 ]},
                                                {'$ne': [ f"${self.TRIPLES_AMOUNT}", 100000 ]},
                                                {'$eq': [ f'${self.STATUS}', self.STATUS_OK]}

                                            ]
                                        }, 
                                        "then": f"${self.TRIPLES_AMOUNT}"
                                    }
                                ],
                                'default': 0
                            }
                        }
                    },
                    "can_get_class_amount": { 
                        "$sum": { 
                            "$switch": { 
                                "branches": [ 
                                    { 
                                        "case": { 
                                            '$and': [
                                                {"$gt": [ f"${self.CLASSES_AMOUNT}", 0 ]},
                                                {'$ne': [ f"${self.CLASSES_AMOUNT}", 10000 ]},
                                                {'$ne': [ f"${self.CLASSES_AMOUNT}", 100000 ]},
                                                {'$eq': [ f'${self.STATUS}', self.STATUS_OK]}
                                            ]
                                        }, 
                                        "then": 1
                                    }
                                ],
                                'default': 0
                            }
                        }
                    },
                    DB.CLASSES_AMOUNT: {
                        "$sum": { 
                            "$switch": { 
                                "branches": [ 
                                    { 
                                        "case": { 
                                            '$and': [
                                                {"$gt": [ f"${self.CLASSES_AMOUNT}", 0 ]},
                                                {'$ne': [ f"${self.CLASSES_AMOUNT}", 10000 ]},
                                                {'$ne': [ f"${self.CLASSES_AMOUNT}", 100000 ]},
                                                {'$eq': [ f'${self.STATUS}', self.STATUS_OK]}

                                            ]
                                        }, 
                                        "then": f"${self.CLASSES_AMOUNT}"
                                    }
                                ],
                                'default': 0
                            }
                        }
                    },
                    "can_get_instance_amount": { 
                        "$sum": { 
                            "$switch": { 
                                "branches": [ 
                                    { 
                                        "case": { 
                                            '$and': [
                                                {"$gt": [ f"${self.INSTANCES_AMOUNT}", 0 ]},
                                                {'$ne': [ f"${self.INSTANCES_AMOUNT}", 10000 ]},
                                                {'$ne': [ f"${self.INSTANCES_AMOUNT}", 100000 ]},
                                                {'$eq': [ f'${self.STATUS}', self.STATUS_OK]}
                                            ]
                                        }, 
                                        "then": 1
                                    }
                                ],
                                'default': 0
                            }
                        }
                    },
                    DB.INSTANCES_AMOUNT: {
                        "$sum": { 
                            "$switch": { 
                                "branches": [ 
                                    { 
                                        "case": { 
                                            '$and': [
                                                {"$gt": [ f"${self.INSTANCES_AMOUNT}", 0 ]},
                                                {'$ne': [ f"${self.INSTANCES_AMOUNT}", 10000 ]},
                                                {'$ne': [ f"${self.INSTANCES_AMOUNT}", 100000 ]},
                                                {'$eq': [ f'${self.STATUS}', self.STATUS_OK]}

                                            ]
                                        }, 
                                        "then": f"${self.INSTANCES_AMOUNT}"
                                    }
                                ],
                                'default': 0
                            }
                        }
                    },
                    "can_get_property_amount": { 
                        "$sum": { 
                            "$switch": { 
                                "branches": [ 
                                    { 
                                        "case": { 
                                            '$and': [
                                                { "$gt": [ f"${self.USED_PROPERTIES_AMOUNT}", 0 ] },
                                                { '$ne': [ f"${self.USED_PROPERTIES_AMOUNT}", 10000 ] },
                                                { '$ne': [ f"${self.USED_PROPERTIES_AMOUNT}", 100000 ] },
                                                { '$eq': [ f'${self.STATUS}', self.STATUS_OK] }
                                            ]
                                        }, 
                                        "then": 1
                                    }
                                ],
                                'default': 0
                            }
                        }
                    },
                    DB.USED_PROPERTIES_AMOUNT: {
                        "$sum": { 
                            "$switch": { 
                                "branches": [ 
                                    { 
                                        "case": { 
                                            '$and': [
                                                {"$gt": [ f"${self.USED_PROPERTIES_AMOUNT}", 0 ]},
                                                {'$ne': [ f"${self.USED_PROPERTIES_AMOUNT}", 10000 ]},
                                                {'$ne': [ f"${self.USED_PROPERTIES_AMOUNT}", 100000 ]},
                                                {'$eq': [ f'${self.STATUS}', self.STATUS_OK]}
                                            ]
                                        }, 
                                        "then": f"${self.USED_PROPERTIES_AMOUNT}"
                                    }
                                ],
                                'default': 0
                            }
                        }
                    },
                    "can_get_unique_subject_amount": { 
                        "$sum": { 
                            "$switch": { 
                                "branches": [ 
                                    { 
                                        "case": {
                                            '$and': [
                                                {"$gt": [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 0 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 100 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 500 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 10000 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 50000 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 100000 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 250000 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 500000 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 1000000 ]},
                                                {'$eq': [ f'${self.STATUS}', self.STATUS_OK]}
                                            ]
                                        }, 
                                        "then": 1
                                    }
                                ],
                                'default': 0
                            }
                        }
                    }, 
                    DB.UNIQUE_SUBJECTS_AMOUNT: {
                        "$sum": { 
                            "$switch": { 
                                "branches": [ 
                                    { 
                                        "case": { 
                                            '$and': [
                                                {"$gt": [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 0 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 100 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 500 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 10000 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 50000 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 100000 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 250000 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 500000 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 1000000 ]},
                                                {'$eq': [ f'${self.STATUS}', self.STATUS_OK]}
                                            ]
                                        }, 
                                        "then": f"${self.UNIQUE_SUBJECTS_AMOUNT}"
                                    }
                                ],
                                'default': 0
                            }
                        }
                    },
                    "can_get_average_unique_subject_amount": { 
                        "$sum": { 
                            "$switch": { 
                                "branches": [ 
                                    { 
                                        "case": {
                                            '$and': [
                                                {"$gt": [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 0 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 100 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 500 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 10000 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 50000 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 100000 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 250000 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 500000 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 1000000 ]},
                                                {"$gt": [ f"${self.TRIPLES_AMOUNT}", 0 ]},
                                                {'$ne': [ f"${self.TRIPLES_AMOUNT}", 10000 ]},
                                                {'$ne': [ f"${self.TRIPLES_AMOUNT}", 100000 ]},
                                                {'$eq': [ f'${self.STATUS}', self.STATUS_OK]}
                                            ]
                                        }, 
                                        "then": 1
                                    }
                                ],
                                'default': 0
                            }
                        }
                    },
                    'totals_triples_for_subjects_avg': {
                        "$sum": { 
                            "$switch": { 
                                "branches": [ 
                                    {
                                        "case": { 
                                            '$and': [
                                                {"$gt": [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 0 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 100 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 500 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 10000 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 50000 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 100000 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 250000 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 500000 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 1000000 ]},
                                                {"$gt": [ f"${self.TRIPLES_AMOUNT}", 0 ]},
                                                {'$ne': [ f"${self.TRIPLES_AMOUNT}", 10000 ]},
                                                {'$ne': [ f"${self.TRIPLES_AMOUNT}", 100000 ]},
                                                {'$eq': [ f'${self.STATUS}', self.STATUS_OK]}
                                            ]
                                        },
                                        "then": f"${self.TRIPLES_AMOUNT}"
                                    }
                                ],
                                'default': 0
                            }
                        }
                    },
                    'totals_unique_subjects_for_subjects_avg': {
                        "$sum": { 
                            "$switch": { 
                                "branches": [ 
                                    {
                                        "case": { 
                                            '$and': [
                                                {"$gt": [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 0 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 100 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 500 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 10000 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 50000 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 100000 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 250000 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 500000 ]},
                                                {'$ne': [ f"${self.UNIQUE_SUBJECTS_AMOUNT}", 1000000 ]},
                                                {"$gt": [ f"${self.TRIPLES_AMOUNT}", 0 ]},
                                                {'$ne': [ f"${self.TRIPLES_AMOUNT}", 10000 ]},
                                                {'$ne': [ f"${self.TRIPLES_AMOUNT}", 100000 ]},
                                                {'$eq': [ f'${self.STATUS}', self.STATUS_OK]}
                                            ]
                                        },
                                        "then": f"${self.UNIQUE_SUBJECTS_AMOUNT}"
                                    }
                                ],
                                'default': 0
                            }
                        }
                    },
                    "is_virtuoso": { 
                        "$sum": { 
                            "$switch": { 
                                "branches": [ 
                                    { 
                                        "case": { "$eq": [ f"${self.QUERY_EDITOR_NAME}", 'Virtuoso' ] }, 
                                        "then": 1
                                    }
                                ],
                                'default': 0
                            }
                        }
                    }
                }
            },
            {
                '$sort': {
                    '_id': 1
                }
            }
        ]

        if separate == True:
            pipeline[0]['$group']['_id'] = f'${self.DOMAINS}'

            pipeline.insert(0, {
                '$unwind': {
                    'path': f'${self.DOMAINS}'
                },
            })

        return self.endpoints.aggregate(pipeline)