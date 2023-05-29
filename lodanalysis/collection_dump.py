import json
from bson.json_util import dumps
from typing import Any

class CollectionDump():
    """ 
    Class for creating and exporting data dumps 
    """
    def export_dump(
            self, 
            output_file_name: str, 
            data: Any
        ) -> bool:
        """ Creates a data dump for a single endpoint """
        json_data = json.loads(dumps(data))

        try:
            with open('dumps/' + output_file_name + '.json', 'w') as file:
                json.dump(json_data, file, indent=4)
        except Exception as e:
            print(e)

            return False
        return True