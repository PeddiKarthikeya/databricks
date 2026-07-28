# Databricks notebook source
# =====================================================================
# setup-catalog  ->  ONE-TIME setup: create the catalog + 3 schemas
# ---------------------------------------------------------------------
# Run this ONCE before the pipeline. The tables themselves are created
# automatically by write_delta() / saveAsTable() when each notebook runs
# -- here we only create the containers they live in.
#
# Medallion layout:  bronze (raw) -> silver (clean) -> gold (aggregated)
# =====================================================================

CATALOG = "payments_catalog"

# 1) create the catalog (top-level container) if it doesn't exist yet
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
print(f"Catalog ready -> {CATALOG}")

# 2) create the 3 medallion schemas (databases) inside the catalog
for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print(f"Schema ready -> {CATALOG}.{schema}")

print("Setup complete. You can now run the pipeline notebooks.")
