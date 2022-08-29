# Databricks notebook source
def customers():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .load("/databricks-datasets/retail-org/customers/")
  )

def sales_orders_raw():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .load("/databricks-datasets/retail-org/sales_orders/")
  )

# COMMAND ----------

data_source = dbutils.widgets.get("data_source")
print("Picking Data from: ", data_source)
source_format = dbutils.widgets.get("source_format")
print("Format of file is:", source_format)
table_name = dbutils.widgets.get("table_name")
print("Loading data into: ", table_name)
checkpoint_path = dbutils.widgets.get("checkpoint_path")
print("Checkpoint Location:", checkpoint_path)

# COMMAND ----------

spark.conf.set('fs.azure.account.key.adlssourabh.dfs.core.windows.net',"G8NRmO/boNiXwKGsuqOc7Aoef/r+5LT2yPWBVcmjF6FvkUKJ8sOaDPTvmEAbx7xe0NSNHCBoAn15+AStNbxmPg==")

# COMMAND ----------

data = spark.read.csv(path="abfss://demolake@adlssourabh.dfs.core.windows.net/1000 Sales Records.csv")

# COMMAND ----------

spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").load("/")

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
          .option("cloudFiles.format", "json")\
          .load("/")

# COMMAND ----------

df.csv('/1000 Sales Records.csv')
