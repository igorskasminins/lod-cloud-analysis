import json
from lodanalysis.mongo_db import DB
from bson.json_util import dumps

# Creating and exporting data dumps
class Dump():
    # Creates a data dump from the endpoint collection
    def export_endpoint_collection_dump(self, output_file_name):
        db = DB()
        cursor = db.get_endpoint_collection()
        json_data = json.loads(dumps(cursor))

        self.export_dump(output_file_name, json_data)

    # Creates a data dump from a dictionary - a single endpoint
    def export_dump(self, output_file_name, json_data):
        with open(output_file_name + '.json', 'w') as file:
            json.dump(json_data, file)