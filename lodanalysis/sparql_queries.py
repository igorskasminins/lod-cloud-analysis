from SPARQLWrapper import SPARQLWrapper, JSON

# Class for setting up the SPARQL wrapper
# and calling different queries with exception handling
class SPARQLQueries:
    RETURN_FORMAT = JSON

    PROPERTY_AMOUNT = 'propAmount'
    PROPERTY = 'property'

    CLASS_AMOUNT = 'classAmount'
    CLASS = 'class'

    def set_wrapper(self, endpoint_name: str) -> None:
        self.wrapper = SPARQLWrapper(endpoint_name)
        self.wrapper.setReturnFormat(self.RETURN_FORMAT)

    # Tests the SPARQL endpoint connection by selecting 10 first triples
    def test_connection(self) -> None:
        self.wrapper.setQuery("""
            SELECT * WHERE {?s ?p ?o} LIMIT 10
            """
        )

        self.wrapper.queryAndConvert()

    # Retrievs the total amount of triples in the dataset
    def get_total_triple_amount(self) -> int:
        try:
            self.wrapper.setQuery(f"""
                SELECT (COUNT(?s) AS ?{self.PROPERTY_AMOUNT}) 
                WHERE {{
                    ?s ?p ?o
                }}
                """
            )

            return int(self.wrapper.queryAndConvert()['results']['bindings'][0][self.PROPERTY_AMOUNT]['value'])
        except:
            return -1

    # Retrievs the total amount of classes in the dataset
    def get_total_class_amount(self):
        try:
            self.wrapper.setQuery(f"""
                SELECT (COUNT (DISTINCT ?type) as ?{self.CLASS_AMOUNT})
                WHERE {{
                    ?s a ?type.
                }}
                """)
            
            return int(self.wrapper.queryAndConvert()['results']['bindings'][0][self.CLASS_AMOUNT]['value'])
        except:
            return -1

    # Retrievs the most used classes in the dataset
    def get_most_used_classes(self):
        try:
            self.wrapper.setQuery(f"""
                SELECT ?{self.CLASS} (COUNT(?instance) AS ?{self.CLASS_AMOUNT}) 
                WHERE {{
                    ?instance a ?{self.CLASS}.
                }}
                GROUP BY ?{self.CLASS}
                ORDER BY DESC(?{self.CLASS_AMOUNT})
                LIMIT 1000
                """
            )

            return self.wrapper.queryAndConvert()['results']['bindings']
        except:
            return []
        
    # Retrievs the most used properties in the dataset
    def get_most_used_properties(self):
        try:
            self.wrapper.setQuery(f"""
                SELECT ?{self.PROPERTY} (COUNT(?property) AS ?{self.PROPERTY_AMOUNT})
                WHERE {{ 
                    ?s ?{self.PROPERTY} ?o.
                }}
                GROUP BY ?{self.PROPERTY}
                ORDER BY DESC(?propTotal)
                """
            )

            return self.wrapper.queryAndConvert()['results']['bindings']
        except:
            return []