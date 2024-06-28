# Databricks notebook source
# MAGIC %md
# MAGIC This notebook generates a computationally expensive spark job of varying length.

# COMMAND ----------

dbutils.widgets.text("iterations", "1000")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import rand

## ADJUST THESE ##
i_iterations = int(dbutils.widgets.get("iterations")) # Every increment by 1 is ~30 seconds of additional runtime
j_iterations = 500


# Don't Touch These
num_rows = 1000000000
num_cols = 2500

join_rows=num_rows

# Create a DataFrame with random values
df = spark.range(num_rows).selectExpr(["rand() as rand_col_" + str(i) for i in range(num_cols)])

# Temp Table
df.createOrReplaceTempView("generated_table")

# Show the DataFrame
display(df)

# COMMAND ----------

# Display a filter
display(spark.sql("select rand_col_3 from generated_table where rand_col_3 > 0.5"))

# COMMAND ----------

# Display another filter
display(spark.sql("select * from generated_table where rand_col_3 !=  rand_col_4"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Display a visualization
# MAGIC select * from generated_table;

# COMMAND ----------

# Display a join
display(spark.sql("select a.rand_col_3 from generated_table a, generated_table b where a.rand_col_3 == b.rand_col_3"))

# COMMAND ----------

# Perform Computationally Expensive Work

import numpy as np

# Function to perform a complex and time-consuming operation
def complex_operation(x):
    result = 0
    for i in range(1,i_iterations):
        for j in range(1,j_iterations):
            #result += np.sqrt(np.log(x) * np.exp(x) / (i + 1) ** 2) 
            result += ((x*i+x*j)^2)/(i+1+j)+((x*i+x*j)^3)/(i+1+j)+((x*i+x*j)^4)/(i+1+j)
    return result

# Registering the UDF
spark.udf.register("complex_operation", complex_operation)

# Apply the UDF to calculate factorial for a few of the columns just so this is a computatioanlly expensive job
df0  = spark.sql("SELECT *, complex_operation(CAST((rand_col_0 * 10) as int)) as factorial_value FROM generated_table")

# Force Compute
df0.show()

# Show the DataFrame
display(df0)


# COMMAND ----------

# Generate random DataFrame 1 with 5 rows and 3 columns
df1 = spark.range(join_rows).selectExpr("id AS id1", "rand() AS value1_1", "rand() AS value1_2")

# Generate random DataFrame 2 with 5 rows and 3 columns
df2 = spark.range(join_rows).selectExpr("id AS id2", "rand() AS value2_1", "rand() AS value2_2")

# Join the two DataFrames on the id column
joined_df = df1.join(df2, df1["id1"] == df2["id2"], "inner")

# Show the result
joined_df.show()

