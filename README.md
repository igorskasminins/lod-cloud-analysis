# **The LOD Cloud Analysis Tool** 

A console application for fetching datasets from [lod-cloud.net](https://lod-cloud.net/), collecting general infromation about the datasets and performing SPARQL queries on the datasets' endpoints. The raw data is being taken from the *Data and code* section on the website that provides a JSON file of the datasets' metadata.

The application is also suitable for fetching data from individual SPARQL endpoints.

## **Installation instructions** 
### **Prerequirements**
- Have MongoDB installed locally
- Python3
- PIP

### **Installation steps**
1. Clone the repository to any place.
2. Run the command from the repository root directory in order to install the required modules:
```
python -m pip install -r requirements.txt
``` 

3. Copy the *`env.ini.sample`* file, rename it to *`env.ini`* and adjust the *DATABASE* section with your relevant port, database and collection name and other settings as you wish if necessary.

<br>

## **Commands**
<br>

Extract data from the LOD Cloud JSON file and performs SPARQL queries on their endpoints:
```
python -m lodanalysis generate
```

Dump the whole endpoint collection in JSON format:
```
python -m lodanalysis dump
```

Analyze SPARQL endpoint by its URL:
```
python -m lodanalysis get
```

Downloads the latest LOD cloud JSON file with raw data:
```
python -m lodanalysis download
```

Drop the endpoint collection:
```
python -m lodanalysis drop
```