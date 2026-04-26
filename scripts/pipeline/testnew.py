from pyspark.sql.functions import col, count
import sys
sys.path.append("/app")
from scripts.utils.connection import sparkConnection

spark = sparkConnection()


def check_duplicate_by_key(spark, path, keys):
    df = spark.read.parquet(path)
    
    print("=== Total Rows ===")
    print(df.count())
    
    print(f"\n=== Duplicate berdasarkan {keys} ===")
    dup = (df.groupBy(keys)
             .count()
             .filter(col("count") > 1))
    
    dup.show(20, False)
    
    print("\n=== Jumlah key yang duplicate ===")
    print(dup.count())
    
    print("\n=== Total row yang terlibat duplicate ===")
    dup_rows = dup.selectExpr(f"sum(count) as total_dup_rows")
    dup_rows.show()
    
    
check_duplicate_by_key(
    spark,
    "./data/gold/fact_sales/",
    ["order_id"]
)


check_duplicate_by_key(spark,
    "./data/gold/fact_sales/",
    ["order_id", "product_id"]
)
check_duplicate_by_key(spark,
    "./data/gold/fact_sales/",
    ["order_id", "customer_id"]
)


def check_exact_duplicate(spark, path):
    df = spark.read.parquet(path)
    
    total = df.count()
    unique = df.dropDuplicates().count()
    
    print(f"Total rows  : {total}")
    print(f"Unique rows : {unique}")
    print(f"Duplicate   : {total - unique}")
    
check_exact_duplicate(spark,"./data/gold/fact_sales/")