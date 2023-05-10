# **The LOD Clound Analysis Tool** 

A console application for fetching datasets from [lod-cloud.net](https://lod-cloud.net/), collecting general infromation about the datasets and performing SPARQL queries on the datasets' endpoints. The data is being taken from the *Data and code* section that provides a JSON file of the datasets' metadata.
<br>
<br>
The application is also suitable for fetching data from individual SPARQL endpoints.

## **Installation instruction** 
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


## **Commands**