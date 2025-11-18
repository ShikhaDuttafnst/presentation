# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingestion Bronze Dataverse
# MAGIC

# COMMAND ----------



import json
import msal
import requests
import pandas as pd
from pyspark.sql import types as T
from pyspark.sql.functions import col
from xml.etree import ElementTree as ET


# COMMAND ----------



# Path to config file in Workspace
CONFIG_PATH = "/Workspace/Users/adm.shikha.dutta@fnst.com/chemical-datahub/Config/tablesconfig.json"

# Load config
with open(CONFIG_PATH.replace("/Workspace", "/Workspace"), "r") as f:  # Adjust path for DBFS if needed
    config = json.load(f)

TARGET_CATALOG = config["catalog"]
TARGET_SCHEMA = config["schema"]
ENTITY_SETS = config["entity_sets"]

print("Loaded configuration:")
print(json.dumps(config, indent=2))

# COMMAND ----------

SECRET_SCOPE = "ChemicalData"

def _dbutils():
    try:
        return dbutils
    except NameError:
        raise RuntimeError("This must run on Databricks.")

def get_cfg():
    cid  = _dbutils().secrets.get(SECRET_SCOPE, "eladb-client-id")
    csec = _dbutils().secrets.get(SECRET_SCOPE, "eladb-client-secret")
    tid  = _dbutils().secrets.get(SECRET_SCOPE, "eladb-tenant-id")
    url  = "https://elaprod.crm4.dynamics.com"
    return cid, csec, tid, url

CLIENT_ID, CLIENT_SECRET, TENANT_ID, DATAVERSE_URL = get_cfg()
RESOURCE = DATAVERSE_URL.rstrip("/")
SCOPE    = f"{RESOURCE}/.default"
API_VER  = "v9.2"

def get_access_token(client_id, client_secret, tenant_id, scope):
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.ConfidentialClientApplication(client_id, client_secret, authority=authority)
    result = app.acquire_token_silent(scopes=[scope], account=None) or app.acquire_token_for_client(scopes=[scope])
    if "access_token" not in result:
        raise RuntimeError(f"Token acquisition failed: {result}")
    return result["access_token"]

token = get_access_token(CLIENT_ID, CLIENT_SECRET, TENANT_ID, SCOPE)
print("Token acquired successfully.")

# COMMAND ----------


CLIENT_ID = config.get("client_id",  "5d4e0f75-2912-4198-99ff-a05ca33460cd")
CLIENT_SECRET = config.get("client_secret",  "g7k8Q~MhJ9ujd~ZZK0P1S~ld8w93rZUM7ct93a8o")
TENANT_ID = config.get("tenant_id",  "2314cb5c-e44b-4288-b205-51ab43ecb122")
DATAVERSE_URL = "https://elaprod.crm4.dynamics.com"

RESOURCE = DATAVERSE_URL.rstrip("/")
SCOPE = f"{RESOURCE}/.default"
API_VER = "v9.2"


# COMMAND ----------

def get_access_token(client_id, client_secret, tenant_id, scope):
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.ConfidentialClientApplication(client_id, client_secret, authority=authority)
    result = app.acquire_token_silent(scopes=[scope], account=None) or app.acquire_token_for_client(scopes=[scope])
    if "access_token" not in result:
        raise RuntimeError(f"Token acquisition failed: {result}")
    return result["access_token"]

token = get_access_token(CLIENT_ID, CLIENT_SECRET, TENANT_ID, SCOPE)
print("Token acquired successfully.")

# COMMAND ----------

SESSION = requests.Session()
SESSION.headers.update({
    "Authorization": f"Bearer {token}",
    "Accept": "application/xml",
    "OData-MaxVersion": "4.0",
    "OData-Version": "4.0"
})

# Dictionary-based mappings
EDM_TO_SPARK = {
    "Edm.String": T.StringType(),
    "Edm.Guid": T.StringType(),
    "Edm.Int32": T.IntegerType(),
    "Edm.Int16": T.IntegerType(),
    "Edm.Int64": T.LongType(),
    "Edm.Decimal": T.DoubleType(),
    "Edm.Double": T.DoubleType(),
    "Edm.Boolean": T.BooleanType(),
    "Edm.DateTimeOffset": T.TimestampType()
}

SPARK_TO_SQL = {
    T.StringType: "STRING",
    T.IntegerType: "INT",
    T.LongType: "BIGINT",
    T.DoubleType: "DOUBLE",
    T.BooleanType: "BOOLEAN",
    T.TimestampType: "TIMESTAMP"
}

def map_edm_to_spark(edm_type: str):
    return EDM_TO_SPARK.get(edm_type, T.StringType())

def spark_type_to_sql(dtype):
    return SPARK_TO_SQL.get(type(dtype), "STRING")


# COMMAND ----------

def get_entity_schema(entity_set: str) -> T.StructType:
    print ("lets get the metadata")
    metadata_url = f"{RESOURCE}/api/data/{API_VER}/$metadata"
    r = SESSION.get(metadata_url, headers={"Accept": "application/xml"})  # Ensure XML
    if r.status_code != 200:
        raise RuntimeError(f"Failed to fetch metadata: {r.text}")
    
    xml = r.text
    root = ET.fromstring(xml)
    ns = {'edmx': 'http://docs.oasis-open.org/odata/ns/edmx', 'edm': 'http://docs.oasis-open.org/odata/ns/edm'}

    struct_fields = []
    found = False 
    print(f"\n--- EDM Types for entity: {entity_set} ---")
    
    for entity in root.findall(".//edm:EntityType", ns):
        if entity.attrib.get('Name') == entity_set:
            found = True  
            for prop in entity.findall("edm:Property", ns):
                name = prop.attrib['Name']
                edm_type = prop.attrib['Type']
                spark_type = map_edm_to_spark(edm_type)
                sql_type = spark_type_to_sql(spark_type)
                print(f"Column: {name}, EDM Type: {edm_type}, Spark Type: {spark_type.simpleString()}, SQL Type: {sql_type}")
                struct_fields.append(T.StructField(name, spark_type, True))

            # Navigation properties (optional)
            for nav in entity.findall("edm:NavigationProperty", ns):
                print(f"Navigation: {nav.attrib['Name']} -> {nav.attrib.get('Type')}")

            # Handle BaseType (inheritance)
            if entity.attrib.get('BaseType'):
                print(f"Inherits from: {entity.attrib['BaseType']} (properties not shown yet)")
    

    if not found:
        print(f"EntityType '{entity_set}' not found. Use list_all_entity_types() to check names.")
    elif not struct_fields:
        print("No direct properties found. This entity may inherit from a base type or only have navigation properties.")
    
    print("--- End of EDM Types ---\n")
    return T.StructType(struct_fields)


