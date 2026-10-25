# Databricks notebook source
# MAGIC %md
# MAGIC # Profiling utilities
# MAGIC Shared helpers used by the profiling notebooks. Load them with `%run ./00_profiling_utils`.

# COMMAND ----------

from pyspark.sql import functions as F

RAW = "/Volumes/olist_dwh/landing/raw"   # change if your CSVs live elsewhere

# COMMAND ----------

def read_raw(file):
    """Read a raw CSV exactly as delivered: every column a string.
    multiLine + escape handle review comments that contain line breaks and quotes.
    Also strips a hidden BOM character (\ufeff) some files carry in their first header."""
    df = (spark.read
          .option("header", True)
          .option("multiLine", True)
          .option("escape", '"')
          .csv(f"{RAW}/{file}"))
    return df.toDF(*[c.replace("\ufeff", "").strip() for c in df.columns])


def null_report(df):
    """Null or blank % per column, worst first."""
    total = df.count()
    if total == 0:
        return spark.createDataFrame([], "column string, null_rows long, null_pct double")
    aggs = [F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), 1).otherwise(0)).alias(c)
            for c in df.columns]
    counts = df.agg(*aggs).first().asDict()
    rows = [(c, int(counts[c]), round(100.0 * counts[c] / total, 2)) for c in df.columns]
    return (spark.createDataFrame(rows, "column string, null_rows long, null_pct double")
            .orderBy(F.desc("null_pct")))


def duplicate_keys(df, keys):
    """Key values that appear more than once."""
    return df.groupBy(*keys).count().filter("count > 1").orderBy(F.desc("count"))


def date_ranges(df, cols):
    """Min / max / unparseable count for timestamp columns stored as strings."""
    rows = []
    for c in cols:
        ts = F.to_timestamp(F.col(c))
        r = df.agg(F.min(ts).alias("min"), F.max(ts).alias("max"),
                   F.sum(F.when(F.col(c).isNotNull() & ts.isNull(), 1).otherwise(0)).alias("unparseable")).first()
        rows.append((c, str(r["min"]), str(r["max"]), int(r["unparseable"] or 0)))
    return spark.createDataFrame(rows, "column string, min string, max string, unparseable long")


def numeric_summary(df, cols):
    """min / max / avg and negative-value count for numeric columns stored as strings."""
    rows = []
    for c in cols:
        v = F.col(c).cast("double")
        r = df.agg(F.min(v).alias("min"), F.max(v).alias("max"), F.avg(v).alias("avg"),
                   F.sum(F.when(v < 0, 1).otherwise(0)).alias("negatives"),
                   F.sum(F.when(F.col(c).isNotNull() & v.isNull(), 1).otherwise(0)).alias("not_numeric")).first()
        rows.append((c, r["min"], r["max"], round(r["avg"], 2) if r["avg"] is not None else None,
                     int(r["negatives"] or 0), int(r["not_numeric"] or 0)))
    return spark.createDataFrame(rows, "column string, min double, max double, avg double, negatives long, not_numeric long")


def distinct_values(df, col, limit=50):
    """Value counts for a categorical column."""
    return df.groupBy(col).count().orderBy(F.desc("count")).limit(limit)


def profile(name, file, keys, date_cols=(), numeric_cols=(), categorical_cols=()):
    """Run every check for one source and display the results."""
    df = read_raw(file).cache()
    total = df.count()
    dups = duplicate_keys(df, keys)
    dup_count = dups.count()

    print(f"===== {name} ({file}) =====")
    print(f"rows: {total:,} | columns: {len(df.columns)} | key {keys}: {dup_count:,} duplicated values")

    display(df.limit(5))
    display(null_report(df))
    if dup_count:
        display(dups.limit(20))
    if date_cols:
        display(date_ranges(df, list(date_cols)))
    if numeric_cols:
        display(numeric_summary(df, list(numeric_cols)))
    for c in categorical_cols:
        display(distinct_values(df, c))

    df.unpersist()
    return (name, total, len(df.columns), dup_count)
