# Databricks notebook source
# MAGIC %md
# MAGIC # Profile all 9 Olist sources
# MAGIC One section per source: row count, null %, duplicate keys, date ranges, numeric ranges, category values.
# MAGIC Write every surprise you see into `docs/profiling.md`.

# COMMAND ----------

# MAGIC %run ./00_profiling_utils

# COMMAND ----------

summary = []

# COMMAND ----------

# MAGIC %md ## 1. Orders

# COMMAND ----------

summary.append(profile(
    "orders", "olist_orders_dataset.csv", keys=["order_id"],
    date_cols=["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
               "order_delivered_customer_date", "order_estimated_delivery_date"],
    categorical_cols=["order_status"]))

# COMMAND ----------

# MAGIC %md ## 2. Order items

# COMMAND ----------

summary.append(profile(
    "order_items", "olist_order_items_dataset.csv", keys=["order_id", "order_item_id"],
    date_cols=["shipping_limit_date"],
    numeric_cols=["order_item_id", "price", "freight_value"]))

# COMMAND ----------

# MAGIC %md ## 3. Payments

# COMMAND ----------

summary.append(profile(
    "payments", "olist_order_payments_dataset.csv", keys=["order_id", "payment_sequential"],
    numeric_cols=["payment_sequential", "payment_installments", "payment_value"],
    categorical_cols=["payment_type", "payment_installments"]))

# COMMAND ----------

# MAGIC %md ## 4. Reviews

# COMMAND ----------

summary.append(profile(
    "reviews", "olist_order_reviews_dataset.csv", keys=["review_id"],
    date_cols=["review_creation_date", "review_answer_timestamp"],
    categorical_cols=["review_score"]))

# COMMAND ----------

# Extra: how many orders have more than one review, and is review_id + order_id unique?
reviews = read_raw("olist_order_reviews_dataset.csv")
display(duplicate_keys(reviews, ["order_id"]).groupBy("count").count().withColumnRenamed("count", "reviews_per_order"))
print("duplicate (review_id, order_id):", duplicate_keys(reviews, ["review_id", "order_id"]).count())

# COMMAND ----------

# MAGIC %md ## 5. Customers

# COMMAND ----------

summary.append(profile(
    "customers", "olist_customers_dataset.csv", keys=["customer_id"],
    categorical_cols=["customer_state"]))

# COMMAND ----------

# Extra: customer_id vs customer_unique_id, and people seen at more than one zip code
customers = read_raw("olist_customers_dataset.csv")
print("distinct customer_id:       ", customers.select("customer_id").distinct().count())
print("distinct customer_unique_id:", customers.select("customer_unique_id").distinct().count())
moved = (customers.groupBy("customer_unique_id")
         .agg(F.countDistinct("customer_zip_code_prefix").alias("zip_codes"))
         .filter("zip_codes > 1"))
print("customers seen at >1 zip code (SCD2 candidates):", moved.count())

# COMMAND ----------

# MAGIC %md ## 6. Products

# COMMAND ----------

summary.append(profile(
    "products", "olist_products_dataset.csv", keys=["product_id"],
    numeric_cols=["product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm",
                  "product_photos_qty"],
    categorical_cols=["product_category_name"]))

# COMMAND ----------

# Extra: look closely at the column names; are any misspelled?
print(read_raw("olist_products_dataset.csv").columns)

# COMMAND ----------

# MAGIC %md ## 7. Sellers

# COMMAND ----------

summary.append(profile(
    "sellers", "olist_sellers_dataset.csv", keys=["seller_id"],
    categorical_cols=["seller_state"]))

# COMMAND ----------

# MAGIC %md ## 8. Geolocation

# COMMAND ----------

summary.append(profile(
    "geolocation", "olist_geolocation_dataset.csv", keys=["geolocation_zip_code_prefix"],
    numeric_cols=["geolocation_lat", "geolocation_lng"],
    categorical_cols=["geolocation_state"]))

# COMMAND ----------

# Extra: same city spelled differently (accents, case)? Example: Sao Paulo variants
geo = read_raw("olist_geolocation_dataset.csv")
display(geo.filter(F.lower("geolocation_city").rlike("^s.o paulo"))
           .groupBy("geolocation_city").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md ## 9. Category translation

# COMMAND ----------

summary.append(profile(
    "category_translation", "product_category_name_translation.csv", keys=["product_category_name"]))

# COMMAND ----------

# MAGIC %md ## Summary

# COMMAND ----------

display(spark.createDataFrame(summary, "source string, rows long, columns int, duplicated_keys long"))
