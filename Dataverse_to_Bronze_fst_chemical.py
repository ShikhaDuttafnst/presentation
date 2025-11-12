
# Databricks notebook source
# MAGIC %md
# MAGIC # Dataverse → Unity Catalog bronze (fst_chemical_Catalog_cpdh)
# MAGIC
# MAGIC This notebook pulls **zz_Recipe** and **zz_components** from Dataverse Web API
# MAGIC and writes them to **fst_chemical_Catalog_cpdh.bronze** as Delta tables.
# MAGIC
# MAGIC **Targets**
# MAGIC - `fst_chemical_Catalog_cpdh.bronze.zz_recipe`
# MAGIC - `fst_chemical_Catalog_cpdh.bronze.zz_components`
# MAGIC
# MAGIC > If your Dataverse entity set names differ (e.g., pluralized), change the `ENTITY_SETS` mapping below.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install libraries (first run per cluster)

# COMMAND ----------

# If needed, uncomment then run once:
# %pip install msal==1.28.0 requests==2.32.3 pandas==2.2.2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration (secrets or widgets)

# COMMAND ----------

from databricks import widgets
import os

SECRET_SCOPE = os.getenv("DATAVERSE_SECRET_SCOPE", "kv")  # change to your scope name

widgets.text("client_id", "")
widgets.text("client_secret", "")
widgets.text("tenant_id", "")
widgets.text("dataverse_url", "")

# Unity Catalog targets
TARGET_CATALOG = "fst_chemical_Catalog_cpdh"
TARGET_SCHEMA  = "bronze"

# Map friendly table names to Dataverse entity sets (adjust if your entity set names differ)
ENTITY_SETS = {
    "zz_recipe": "zz_Recipe",
    "zz_components": "zz_components"
}

def _dbutils():
    try:
        return dbutils
    except NameError:
        raise RuntimeError("This notebook must run on Databricks (dbutils not found).")

def get_cfg():
    try:
        cid  = _dbutils().secrets.get(SECRET_SCOPE, "dataverse-client-id")
        csec = _dbutils().secrets.get(SECRET_SCOPE, "dataverse-client-secret")
        tid  = _dbutils().secrets.get(SECRET_SCOPE, "dataverse-tenant-id")
        try:
            url = _dbutils().secrets.get(SECRET_SCOPE, "dataverse-url")
        except Exception:
            url = None
    except Exception:
        cid = csec = tid = url = None

    cid  = cid  or widgets.get("client_id")
    csec = csec or widgets.get("client_secret")
    tid  = tid  or widgets.get("tenant_id")
    url  = url  or widgets.get("dataverse_url")

    missing = [k for k,v in {"client_id":cid,"client_secret":csec,"tenant_id":tid,"dataverse_url":url}.items() if not v]
    if missing:
        raise ValueError(f"Missing config: {missing}. Fill widgets or configure secrets in scope '{SECRET_SCOPE}'.")
    return cid, csec, tid, url

CLIENT_ID, CLIENT_SECRET, TENANT_ID, DATAVERSE_URL = get_cfg()
RESOURCE = DATAVERSE_URL.rstrip("/")
SCOPE    = f"{RESOURCE}/.default"
API_VER  = "v9.2"

print("Config OK for resource:", RESOURCE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Authenticate (MSAL client credentials)

# COMMAND ----------

import msal

def get_access_token(client_id, client_secret, tenant_id, scope):
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.ConfidentialClientApplication(
        client_id=client_id,
        client_credential=client_secret,
        authority=authority
    )
    result = app.acquire_token_silent(scopes=[scope], account=None)
    if not result:
        result = app.acquire_token_for_client(scopes=[scope])
    if "access_token" not in result:
        raise RuntimeError(f"Token acquisition failed: {result}")
    return result["access_token"]

token = get_access_token(CLIENT_ID, CLIENT_SECRET, TENANT_ID, SCOPE)
print("Token acquired.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataverse helpers (OData v4)

# COMMAND ----------

import requests, time, pandas as pd
from typing import Optional, List, Dict, Any, Iterable

SESSION = requests.Session()
SESSION.headers.update({
    "Authorization": f"Bearer {token}",
    "Accept": "application/json",
    "OData-MaxVersion": "4.0",
    "OData-Version": "4.0"
})

def _backoff(attempt:int, base:float=1.0, cap:float=30.0):
    import time as _t
    _t.sleep(min(cap, base*(2**(attempt-1))))

def dataverse_get(url: str, params: Optional[Dict[str,str]]=None, max_retries:int=5) -> Dict[str,Any]:
    for a in range(1, max_retries+1):
        r = SESSION.get(url, params=params, timeout=60)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429,500,502,503,504):
            _backoff(a); continue
        try:
            details = r.json()
        except Exception:
            details = r.text
        raise RuntimeError(f"GET failed {r.status_code}: {details}")
    raise RuntimeError("Exceeded retries for GET.")

def build_entity_url(entity_set:str, select:Optional[List[str]]=None, filter_:Optional[str]=None, top:Optional[int]=None) -> str:
    base = f"{RESOURCE}/api/data/{API_VER}/{entity_set}"
    params = []
    if select: params.append(f"$select={','.join(select)}")
    if filter_: params.append(f"$filter={filter_}")
    if top: params.append(f"$top={int(top)}")
    return base + ("?" + "&".join(params) if params else "")

def fetch_all(entity_set:str, select:Optional[List[str]]=None, filter_:Optional[str]=None, page_size:int=5000) -> Iterable[Dict[str,Any]]:
    url = build_entity_url(entity_set, select=select, filter_=filter_, top=page_size)
    while True:
        data = dataverse_get(url)
        for row in data.get("value", []):
            yield row
        next_link = data.get("@odata.nextLink")
        if not next_link:
            break
        url = next_link

from pyspark.sql import types as T

def dataverse_to_spark(entity_set:str, select:Optional[List[str]]=None, filter_:Optional[str]=None, sample_rows:int=1000):
    # sample for schema
    sample = []
    it = fetch_all(entity_set, select=select, filter_=filter_)
    for row in it:
        sample.append(row)
        if len(sample) >= sample_rows:
            break
    if not sample:
        return spark.createDataFrame([], T.StructType([]))

    pdf = pd.json_normalize(sample).astype("string")
    sdf = spark.createDataFrame(pdf)

    # rest
    rest = list(fetch_all(entity_set, select=select, filter_=filter_))
    if rest:
        pdf2 = pd.json_normalize(rest).astype("string")
        sdf2 = spark.createDataFrame(pdf2)
        sdf = sdf.unionByName(sdf2, allowMissingColumns=True)
    return sdf

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Unity Catalog schema

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS `{TARGET_CATALOG}`")
spark.sql(f"CREATE SCHEMA  IF NOT EXISTS `{TARGET_CATALOG}`.`{TARGET_SCHEMA}`")

# Use the target catalog & schema for the session (optional)
spark.sql(f"USE CATALOG `{TARGET_CATALOG}`")
spark.sql(f"USE SCHEMA `{TARGET_SCHEMA}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pull and write the two tables

# COMMAND ----------

# Adjust `$select` as needed to trim columns; leaving None pulls all available fields on first page (with annotations).
recipe_entity     = ENTITY_SETS["zz_recipe"]
components_entity = ENTITY_SETS["zz_components"]

recipe_df     = dataverse_to_spark(recipe_entity, select=None, filter_=None)
components_df = dataverse_to_spark(components_entity, select=None, filter_=None)

display(recipe_df.limit(20))
display(components_df.limit(20))

# Write as managed Delta tables in UC bronze schema
recipe_df.write.format("delta").mode("overwrite").saveAsTable("zz_recipe")
components_df.write.format("delta").mode("overwrite").saveAsTable("zz_components")

print("Wrote tables:")
print(f" - {TARGET_CATALOG}.{TARGET_SCHEMA}.zz_recipe")
print(f" - {TARGET_CATALOG}.{TARGET_SCHEMA}.zz_components")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optional: Incremental pattern
# MAGIC Use a filter like `modifiedon ge <ts>` and then MERGE into target tables.
# MAGIC Change `select` to project only the columns you need.

