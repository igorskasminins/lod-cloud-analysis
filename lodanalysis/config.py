import configparser

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