import configparser

# Wrapper for the application's configs
class Config:
    DATABASE_SECTION_CONFIG = 'DATABASE'
    FILES_SECTION_CONFIG = 'FILES'
    LOD_CLOUD_SECTION_CONFIG = 'LOD_CLOUD'

    def __init__(self):
        self.config_parser = configparser.ConfigParser()
        self.config_parser.read('env.ini')

    # Returns database configuration value by the config path
    def get_db_config(self, config_name: str) -> str:
        return self.config_parser[self.DATABASE_SECTION_CONFIG][config_name]

    # Returns file configuration value by the config path
    def get_file_config(self, config_name: str) -> str:
        return self.config_parser[self.FILES_SECTION_CONFIG][config_name]
    
    # Returns app general configuration value by the config path
    def get_lod_cloud_config(self, config_name: str) -> str:
        return self.config_parser[self.LOD_CLOUD_SECTION_CONFIG][config_name]