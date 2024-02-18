# Databricks notebook source
import zlib
from base64 import urlsafe_b64encode as b64e, urlsafe_b64decode as b64d
def unobscure(obscured):
    credential = zlib.decompress(b64d(obscured))
    credential = credential.decode('utf-8')
    # print(credential)
    return credential

# COMMAND ----------

obscuere_access_key = "eNrTNgjK0U8LD7R01Nc297R0T00MCjYP9qssDCsKSDQyT_H0NfAOyMqqyC0LK7WosAwqSfM3DDJJ9HHLqUiJSMwuLK1MzHXLqywvsAx01HYMLvEyd9Y2KLe1BQDhxRzC"
spark.conf.set("fs.azure.account.key.karadlsdev.dfs.core.windows.net",unobscure(obscuere_access_key))

# COMMAND ----------

display(dbutils.fs.ls("abfss://processed@karadlsdev.dfs.core.windows.net"))

# COMMAND ----------

# Crete DB and location
# db_location = "abfss://processed@karadlsdev.dfs.core.windows.net/rna_qa"
# dbutils.fs.rm(db_location)
spark.sql(f"""
            create database if not exists rna_qa
            --managed location '{db_location}'
            """)
