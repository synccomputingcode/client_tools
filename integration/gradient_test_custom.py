# Databricks notebook source
# MAGIC %md
# MAGIC This notebook generates a computationally expensive spark job of varying length.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import rand

# Define the number of rows and columns
num_rows = 10000000
num_cols = 25

# Create a DataFrame with random values
df = spark.range(num_rows).selectExpr(["rand() as rand_col_" + str(i) for i in range(num_cols)])

# Temp Table
df.createOrReplaceGlobalTempView("generated_table")

# Show the DataFrame
display(df)

# COMMAND ----------

# Display a filter
display(spark.sql("select rand_col_3 from global_temp.generated_table where rand_col_3 > 0.5"))

# COMMAND ----------

# Display another filter
display(spark.sql("select * from global_temp.generated_table where rand_col_3 !=  rand_col_4"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Display a visualization
# MAGIC select * from global_temp.generated_table;

# COMMAND ----------

# Display a join
display(spark.sql("select a.rand_col_3 from global_temp.generated_table a, global_temp.generated_table b where a.rand_col_3 == b.rand_col_3"))

# COMMAND ----------

# Perform Computationally Expensive Work

import numpy as np

# ADJUST THESE
i_iterations = 6
j_interations = 500

# Function to perform a complex and time-consuming operation
def complex_operation(x):
    result = 0
    for i in range(i_iterations):
        for j in range(j_interations):
            result += np.sqrt(np.log(x) * np.exp(x) / (i + 1) ** 2) 
    return result

# Registering the UDF
spark.udf.register("complex_operation", complex_operation)

# Apply the UDF to calculate factorial for a few of the columns just so this is a computatioanlly expensive job
df0  = spark.sql("SELECT *, complex_operation(CAST((rand_col_0 * 10) as int)) as factorial_value FROM global_temp.generated_table")

# Force Compute
df0.show()

# Show the DataFrame
display(df0)


# COMMAND ----------


