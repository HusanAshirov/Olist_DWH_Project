# Databricks notebook source
# MAGIC %md
# MAGIC # Referential integrity and business-rule checks
# MAGIC Each check returns the rows that break a relationship or rule. A count of 0 = clean.

# COMMAND ----------

# MAGIC %run ./00_profiling_utils

# COMMAND ----------

orders    = read_raw("olist_orders_dataset.csv").cache()
items     = read_raw("olist_order_items_dataset.csv").cache()
payments  = read_raw("olist_order_payments_dataset.csv")
reviews   = read_raw("olist_order_reviews_dataset.csv")
customers = read_raw("olist_customers_dataset.csv")
products  = read_raw("olist_products_dataset.csv").cache()
sellers   = read_raw("olist_sellers_dataset.csv")
geo       = read_raw("olist_geolocation_dataset.csv")
trans     = read_raw("product_category_name_translation.csv")

geo_zips = geo.select("geolocation_zip_code_prefix").distinct()
ts = F.to_timestamp

# COMMAND ----------

checks = [
    # --- orphans: child rows whose parent is missing ---
    ("order_items without an order",       items.join(orders, "order_id", "left_anti")),
    ("payments without an order",          payments.join(orders, "order_id", "left_anti")),
    ("reviews without an order",           reviews.join(orders, "order_id", "left_anti")),
    ("orders without a customer",          orders.join(customers, "customer_id", "left_anti")),
    ("order_items with unknown product",   items.join(products, "product_id", "left_anti")),
    ("order_items with unknown seller",    items.join(sellers, "seller_id", "left_anti")),

    # --- parents with no children ---
    ("orders with no items",               orders.join(items, "order_id", "left_anti")),
    ("orders with no payment",             orders.join(payments, "order_id", "left_anti")),
    ("orders with no review",              orders.join(reviews, "order_id", "left_anti")),

    # --- lookups that will not resolve ---
    ("products with no category",          products.filter(F.col("product_category_name").isNull())),
    ("categories missing from translation",
        products.filter(F.col("product_category_name").isNotNull())
                .select("product_category_name").distinct()
                .join(trans, "product_category_name", "left_anti")),
    ("customer zip not in geolocation",
        customers.join(geo_zips.withColumnRenamed("geolocation_zip_code_prefix", "customer_zip_code_prefix"),
                       "customer_zip_code_prefix", "left_anti")),
    ("seller zip not in geolocation",
        sellers.join(geo_zips.withColumnRenamed("geolocation_zip_code_prefix", "seller_zip_code_prefix"),
                     "seller_zip_code_prefix", "left_anti")),

    # --- business rules ---
    ("delivered orders with no delivery date",
        orders.filter((F.col("order_status") == "delivered") & F.col("order_delivered_customer_date").isNull())),
    ("delivered before purchased",
        orders.filter(ts("order_delivered_customer_date") < ts("order_purchase_timestamp"))),
    ("approved before purchased",
        orders.filter(ts("order_approved_at") < ts("order_purchase_timestamp"))),
    ("items with price <= 0",              items.filter(F.col("price").cast("double") <= 0)),
    ("payments with value <= 0",           payments.filter(F.col("payment_value").cast("double") <= 0)),
    ("payments with 0 installments",       payments.filter(F.col("payment_installments").cast("int") == 0)),
    ("payment_type 'not_defined'",         payments.filter(F.col("payment_type") == "not_defined")),
]

# COMMAND ----------

results = spark.createDataFrame([(name, df.count()) for name, df in checks],
                                "check string, failing_rows long")
display(results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Payments vs items: do the totals match per order?
# MAGIC Revenue in gold comes from items (price + freight). Payments should add up to the same amount.

# COMMAND ----------

item_totals = (items.groupBy("order_id")
               .agg(F.sum(F.col("price").cast("double") + F.col("freight_value").cast("double")).alias("items_total")))
pay_totals = (payments.groupBy("order_id")
              .agg(F.sum(F.col("payment_value").cast("double")).alias("payments_total")))
diff = (item_totals.join(pay_totals, "order_id")
        .withColumn("diff", F.round(F.col("payments_total") - F.col("items_total"), 2))
        .filter(F.abs("diff") > 0.01))
print("orders where payments != items + freight:", diff.count())
display(diff.orderBy(F.desc(F.abs("diff"))).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drill into any failing check
# MAGIC Change the name below to see sample rows for that check.

# COMMAND ----------

check_name = "categories missing from translation"
display(dict(checks)[check_name].limit(50))
