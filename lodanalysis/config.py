import configparser
import os

class Config:
    """ 
    Wrapper class for the application's configs 
    """
    DATABASE_SECTION_CONFIG = 'DATABASE'
    FILES_SECTION_CONFIG = 'FILES'
    LOD_CLOUD_SECTION_CONFIG = 'LOD_CLOUD'

    def __init__(self):
        self.config_parser = configparser.ConfigParser()
        self.config_parser.read('env.ini')

    def get_db_config(
            self,
            config_name: str
        ) -> str:
        """ Returns database configuration value by the config path """
        return self.config_parser[self.DATABASE_SECTION_CONFIG][config_name]

    def get_file_config(
            self,
            config_name: str
        ) -> str:
        """ Returns file configuration value by the config path """
        return self.config_parser[self.FILES_SECTION_CONFIG][config_name]

    def get_lod_cloud_config(
            self, 
            config_name: str
        ) -> str:
        """ Returns app general configuration value by the config path """
        return self.config_parser[self.LOD_CLOUD_SECTION_CONFIG][config_name]

    def get_dir_content(
            self,
            dir_path: str
        ) -> list:
        """ Get content of the relevant directory """
        try:
            return os.listdir(dir_path)
        except Exception:
            return []

    def get_dir_path(
            self,
            dir_name: str
        ) -> str:
        """ Gets the absolute path to the relevant directory """
        return os.getcwd() + os.sep + self.get_file_config('queries_directory') + os.sep + dir_name
    
    def check_dir(
            self,
            dir_name: str
        ) -> bool:
        """ Verifies that the directory exists and is not empty """
        dir_path = self.get_dir_path(dir_name)

        return len(self.get_dir_content(dir_path)) > 0

    def get_custom_queries_names(
            self, 
            directory
        ) -> list:
        sparql_queries = []
        """ Gets custom queries' names form the relevant directory """
        dir_path = self.get_dir_path(directory)
        dir_content = self.get_dir_content(dir_path)

        for path in dir_content:
            file_name, _ = os.path.splitext(path)
            sparql_queries.append(file_name)

        return sparql_queries
    
    def get_custom_queries(
            self,
            directory: str = ""
        ) -> list:
        """ Gets custom queries form the relevant directory """
        sparql_queries = []
        dir_path = self.get_dir_path(directory)
        dir_content = self.get_dir_content(dir_path)

        for path in dir_content:
            file_path = os.path.join(dir_path, path)
            if os.path.isfile(file_path):
                file_name, file_extension = os.path.splitext(path)

                if file_extension != '.sparql':
                    continue

                with open(file_path, 'r') as file:
                    sparql_body = "".join(file.readlines())
                    sparql_queries.append(
                        {
                            'name': file_name,
                            'body': sparql_body
                        }
                    )

        return sparql_queries
    