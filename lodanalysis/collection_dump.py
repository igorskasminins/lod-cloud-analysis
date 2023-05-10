import json
from lodanalysis.mongo_db import DB
from bson.json_util import dumps
from typing import Any

class CollectionDump():
    """ Creating and exporting data dumps """
    def export_endpoint_collection_dump(
            self, 
            output_file_name: str, 
            only_active: bool
        ) -> bool:
        """ Creates a data dump from the endpoint collection """
        db = DB()
        collection = db.get_endpoint_collection(only_active)
        json_data = json.loads(dumps(collection))

        return self.export_dump(output_file_name, json_data)

    def export_dump(
            self, 
            output_file_name: str, 
            json_data: Any
        ) -> bool:
        """ Creates a data dump for a single endpoint """
        try:
            with open(output_file_name + '.json', 'w') as file:
                json.dump(json_data, file, indent=4)
        except Exception as e:
            print(e)

            return False
        return True