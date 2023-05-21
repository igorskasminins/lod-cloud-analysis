from SPARQLWrapper import SPARQLWrapper, JSON
from typing import Dict, Any

class SPARQLQueries:
    """ 
    Class for setting up SPARQL wrapper and calling queries 
    """
    RETURN_FORMAT = JSON

    PROPERTY_AMOUNT = 'propAmount'
    PROPERTY = 'property'

    CLASS_AMOUNT = 'classAmount'
    CLASS = 'class'

    ERROR_NUMBER = -1
    ERROR_ARRAY = []

    DEFAULT_TIMEOUT = '120000'

    def set_wrapper(
            self,
            endpoint_name: str
        ) -> None:
        """ Sets the endpoint access URL and the return format for making queries """ 
        self.wrapper = SPARQLWrapper(endpoint_name)
        self.wrapper.setReturnFormat(self.RETURN_FORMAT)

    def set_timeout(
            self, 
            amount: str
        ) -> None:
        self.wrapper.addExtraURITag('timeout', amount)

    def __test_result(self, result):
        if len(result) == 0:
            raise Exception("Empty result")

    def test_connection(self) -> None:
        """ Tests the SPARQL endpoint connection by selecting 10 first triples """
        self.wrapper.setQuery("""
            SELECT * WHERE {?s ?p ?o} LIMIT 10
            """
        )
        result = self.wrapper.queryAndConvert()

        try:
            ok = 'results' in result
        except:
            raise Exception("Output result is not valid")
        
    def get_total_triple_amount(self) -> int:
        """ Retrievs the total amount of triples in the dataset """
        try:
            self.wrapper.setQuery(f"""
                SELECT (COUNT(?s) AS ?{self.PROPERTY_AMOUNT}) 
                WHERE {{
                    ?s ?p ?o
                }}
                """
            )

            result = self.wrapper.queryAndConvert()['results']['bindings']
            self.__test_result(result)

            return int(result[0][self.PROPERTY_AMOUNT]['value'])
        except Exception as e:
            pass
        
        try:
            self.wrapper.setQuery("""
                SELECT ?s
                WHERE {
                    ?s ?p ?o
                }
                """
            )

            result = self.wrapper.queryAndConvert()['results']['bindings']
            self.__test_result(result)

            return len(result)
        except Exception:
            pass

        try:
            self.wrapper.setQuery("""
                SELECT ?s
                WHERE {
                    ?s ?p ?o
                }
                LIMIT 10000
                """
            )

            result = self.wrapper.queryAndConvert()['results']['bindings']
            self.__test_result(result)

            return len(result)
        except Exception:
            return self.ERROR_NUMBER

    def get_total_class_amount(self) -> int:
        """ Retrievs the total amount of classes in the dataset """
        try:
            self.wrapper.setQuery(f"""
                SELECT (COUNT (DISTINCT ?type) as ?{self.CLASS_AMOUNT})
                WHERE {{
                    ?s a ?type.
                }}
                """)

            return int(self.wrapper.queryAndConvert()['results']['bindings'][0][self.CLASS_AMOUNT]['value'])
        except Exception:
            return self.ERROR_NUMBER

    def get_used_classes(self) -> list:
        """ Retrievs the most used classes in the dataset """
        try:
            self.wrapper.setQuery(f"""
                SELECT ?{self.CLASS} (COUNT(?instance) AS ?{self.CLASS_AMOUNT}) 
                WHERE {{
                    ?instance a ?{self.CLASS}.
                }}
                GROUP BY ?{self.CLASS}
                ORDER BY DESC(?{self.CLASS_AMOUNT})
                """
            )

            result = self.wrapper.queryAndConvert()['results']['bindings']
            self.__test_result(result)

            return { 'is_valid': True, 'value': result }
        except Exception:
            pass
    
        try:
            self.wrapper.setQuery(f"""
                SELECT ?{self.CLASS} (COUNT(?instance) AS ?{self.CLASS_AMOUNT}) 
                WHERE {{
                    ?instance a ?{self.CLASS}.
                }}
                GROUP BY ?{self.CLASS}
                ORDER BY DESC(?{self.CLASS_AMOUNT})
                LIMIT 10000
                """
            )

            result = self.wrapper.queryAndConvert()['results']['bindings']
            self.__test_result(result)

            return { 'is_valid': True, 'value': result }
        except Exception:
            pass
        
        try:
            self.wrapper.setQuery(f"""
                SELECT DISTINCT ?{self.CLASS} ?{self.CLASS_AMOUNT}
                WHERE {{
                    ?instance a ?{self.CLASS}.
                    BIND (0 as ?{self.CLASS_AMOUNT})
                }}
                """
            )

            result = self.wrapper.queryAndConvert()['results']['bindings']
            self.__test_result(result)

            return { 'is_valid': True, 'value': result }
        except Exception:
            pass
        
        try:
            self.wrapper.setQuery(f"""
                SELECT DISTINCT ?{self.CLASS} ?{self.CLASS_AMOUNT}
                WHERE {{
                    ?instance a ?{self.CLASS}.
                    BIND (0 as ?{self.CLASS_AMOUNT})
                }}
                LIMIT 10000
                """
            )

            result = self.wrapper.queryAndConvert()['results']['bindings']
            self.__test_result(result)

            return { 'is_valid': True, 'value': result }
        except Exception:
            return { 'is_valid': False }

    def get_used_properties(self) -> Dict[str, Any]:
        """ Retrievs the most used properties in the dataset """
        try:
            self.wrapper.setQuery(f"""
                SELECT ?{self.PROPERTY} (COUNT(?{self.PROPERTY}) AS ?{self.PROPERTY_AMOUNT})
                WHERE {{
                    ?s ?{self.PROPERTY} ?o.
                }}
                GROUP BY ?{self.PROPERTY}
                ORDER BY DESC(?{self.PROPERTY_AMOUNT})
                """
            )

            result = self.wrapper.queryAndConvert()['results']['bindings']
            self.__test_result(result)

            return { 'is_valid': True, 'value': result }
        except Exception:
            pass

        try:
            self.wrapper.setQuery(f"""
                SELECT ?{self.PROPERTY} (COUNT(?{self.PROPERTY}) AS ?{self.PROPERTY_AMOUNT})
                WHERE {{
                    ?s ?{self.PROPERTY} ?o.
                }}
                GROUP BY ?{self.PROPERTY}
                ORDER BY DESC(?{self.PROPERTY_AMOUNT})
                LIMIT 10000
                """
            )

            result = self.wrapper.queryAndConvert()['results']['bindings']
            self.__test_result(result)

            return { 'is_valid': True, 'value': result }
        except Exception:
            pass

        try:
            self.wrapper.setQuery(f"""
                SELECT DISTINCT ?{self.PROPERTY} ?{self.PROPERTY_AMOUNT}
                WHERE {{
                    ?s ?{self.PROPERTY} ?o.
                    BIND(0 as ?{self.PROPERTY_AMOUNT})
                }}
                """
            )

            result = self.wrapper.queryAndConvert()['results']['bindings']
            self.__test_result(result)

            return { 'is_valid': True, 'value': result }
        except Exception:
            pass

        try:
            self.wrapper.setQuery(f"""
                SELECT DISTINCT ?{self.PROPERTY} ?{self.PROPERTY_AMOUNT}
                WHERE {{
                    ?s ?{self.PROPERTY} ?o.
                    BIND(0 as ?{self.PROPERTY_AMOUNT})
                }}
                LIMIT 10000
                """
            )

            result = self.wrapper.queryAndConvert()['results']['bindings']
            self.__test_result(result)

            return { 'is_valid': True, 'value': result }
        except Exception:
            return { 'is_valid': False }

    def get_custom_query_result(
            self,
            query: str
        ) -> Any:
        self.wrapper.setQuery(query)

        try:
            return self.wrapper.queryAndConvert()['results']['bindings']
        except Exception as e:
            print(e)
            return self.ERROR_NUMBER