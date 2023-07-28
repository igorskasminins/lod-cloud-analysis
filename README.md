# **The LOD Cloud Analysis Tool** 

A console application for fetching datasets from [lod-cloud.net](https://lod-cloud.net/), collecting general infromation about the datasets and performing SPARQL queries on the datasets' endpoints. The raw data is being taken from the *Data and code* section on the website that provides a JSON file of the datasets' metadata.

The application is also suitable for fetching data from individual SPARQL endpoints.

## **Installation instructions** 
### **Prerequirements**
- Have MongoDB installed locally
- Python3
- PIP

### **Installation steps**
1. Clone the repository.
2. Run the command from the repository root directory in order to install the required modules:
```
python3 -m pip install -r requirements.txt
``` 

3. Copy the `env.ini.sample` file, rename it to `env.ini` and adjust the *DATABASE* section with your relevant port and database name.

<br>

## **Commands**
<br>

Extract data from the LOD Cloud JSON file and perform SPARQL queries on their endpoints:
```
python3 -m lodanalysis generate
```

Performs custom queries on existing stored active endpoint; appends new or replaces all existing result based on the query names:
```
python3 -m lodanalysis generate-custom
```

Adds an empty endpoint record to the database so that it could be skipper during the generation process from lod-cloud.net:
```
python3 -m lodanalysis skip
```

Dump the whole endpoint collection in JSON format:
```
python3 -m lodanalysis dump
```

Analyze SPARQL endpoint by its URL:
```
python3 -m lodanalysis get
```

Gets all endpoints that are being skipped during analysis of endpoints from lod-cloud.net:
```
python3 -m lodanalysis get-skipped
```

Gets statistics on endpoints:
```
python3 -m lodanalysis get-stats
```

Gets general fields' data from endpoints:
```
python3 -m lodanalysis get-totals
```

Retrieves the most used classes accross all endpoints:
```
python3 -m lodanalysis top-classes
```

Retrieves the most used properties accross all endpoints:
```
python3 -m lodanalysis top-properties
```

Downloads the latest LOD cloud JSON file with raw data:
```
python3 -m lodanalysis download
```

Deletes a single endpoint by access_url:
```
python3 -m lodanalysis delete
```

Delete results from all queries in the specified directory accross all endpoints:
```
python3 -m lodanalysis delete-dir-queries
```

Delete results from a single query accross all endpoints:
```
python3 -m lodanalysis delete-query
```

Drop the endpoint collection:
```
python3 -m lodanalysis drop
```