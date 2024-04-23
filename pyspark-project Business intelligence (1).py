# Databricks notebook source
spark

# COMMAND ----------

from pyspark.sql.types import StringType, StructField, StructType, IntegerType, DateType
from pyspark.sql.functions import year, month, quarter
from pyspark.sql.functions import sum, col, count, countDistinct

# COMMAND ----------

sales_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("location", StringType(), True),
    StructField("source_order", StringType(), True)
])

# COMMAND ----------

sales_df = spark.read.format("csv")\
    .option("header","false")\
    .option("inferSchema","false")\
    .schema(sales_schema)\
    .load("/FileStore/tables/sales_csv.txt")

# COMMAND ----------

sales_df.show(5)

# COMMAND ----------

sales_df = sales_df.withColumn("order_year", year(sales_df.order_date))

# COMMAND ----------

sales_df.show(5)

# COMMAND ----------

sales_df.printSchema()

# COMMAND ----------

sales_df = sales_df.withColumn("order_month", month(sales_df.order_date))
sales_df = sales_df.withColumn("order_quater", quarter(sales_df.order_date))

# COMMAND ----------

sales_df.show(5)

# COMMAND ----------

menu_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("prize", StringType(), True),
])


menu_df = spark.read.format("csv")\
    .option("header","false")\
    .option("inferSchema","true")\
    .schema(menu_schema)\
    .load("/FileStore/tables/menu_csv.txt")

# COMMAND ----------

menu_df.show()

# COMMAND ----------

sales_df.show(10)

# COMMAND ----------

menu_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Total amount spend by each customer

# COMMAND ----------

total_amount_spend = (sales_df.join(menu_df, 'product_id'). groupBy('customer_id').agg(sum(col('prize').cast('float')).alias('total_sum')).orderBy('customer_id'))
display(total_amount_spend)

# COMMAND ----------

# MAGIC %md
# MAGIC Total amount sell in each month

# COMMAND ----------

each_month_sell = (sales_df.join(menu_df,('product_id')).groupBy('order_month').agg(sum(col('prize').cast('float')).alias('Total_sell')).orderBy('order_month')
                   )

display(each_month_sell)

# COMMAND ----------

each_month_sell.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Total amount spend by each food category

# COMMAND ----------

each_food_sell = sales_df.join(menu_df,'product_id').groupBy('product_id','product_name').agg(sum(col('prize').cast('float')).alias('total_sell')).drop('product_id')

display(each_food_sell)

# COMMAND ----------

# MAGIC %md
# MAGIC Yearly Sale

# COMMAND ----------

yearly_sale = sales_df.join(menu_df,'product_id').groupBy('order_year').agg(sum(col('prize').cast('float')).alias('total_sell'))

display(yearly_sale)

# COMMAND ----------

quaterly_sale = sales_df.join(menu_df,'product_id').groupBy('order_quater').agg(sum(col('prize').cast('float')).alias('total_sell')).orderBy('order_quater')

display(quaterly_sale)

# COMMAND ----------

# MAGIC %md
# MAGIC Total number of order by each category

# COMMAND ----------

sell_by_each_product = sales_df.join(menu_df,'product_id').groupBy('product_id', 'product_name').agg(count('product_id').alias('product_count')).orderBy('product_count', ascending = 0).drop('product_id')

display(sell_by_each_product)

# COMMAND ----------

# MAGIC %md
# MAGIC Top ordered item

# COMMAND ----------

order_top_product = sales_df.join(menu_df,'product_id').groupBy('product_name').agg(count('product_id').alias('order_count')).orderBy('order_count', ascending = 0).limit(1)
display(order_top_product)

# COMMAND ----------

# MAGIC %md
# MAGIC Frequency of customer visited to Restourant

# COMMAND ----------

customer_visited_restourant = sales_df.filter(sales_df.source_order == 'Restaurant').groupBy('customer_id').agg(countDistinct('order_date').alias('total visit'))
display(customer_visited_restourant)

# COMMAND ----------

# MAGIC %md
# MAGIC  total sales by each country

# COMMAND ----------

sales_each_country = sales_df.join(menu_df,'product_id').groupBy('location').agg(sum(col('prize').cast('float')).alias('Total sum'))

display(sales_each_country)


# COMMAND ----------

# MAGIC %md
# MAGIC total sales by each source

# COMMAND ----------

sales_each_source = sales_df.join(menu_df,'product_id').groupBy('source_order').agg(sum(col('prize').cast('float')).alias('Total sum'))

display(sales_each_source)
