from typing import Optional, Tuple
from pyspark.sql import DataFrame

catalog = {catalog}
bronze_schema = {bronze_schema}

def next_snapshot_and_version(latest_snapshot_version: Optional[int]) -> Optional[Tuple[DataFrame, int]]:

    if latest_snapshot_version is None:
        df = spark.sql("""
            SELECT * FROM {catalog}.{bronze_schema}.part 
            WHERE snapshot_id = (SELECT min(snapshot_id) FROM {catalog}.{bronze_schema}.part)
        """)
        
        min_snapshot_id = spark.sql("""
            SELECT min(snapshot_id) as min_id FROM {catalog}.{bronze_schema}.part
        """).collect()[0].min_id
        
        return (df, min_snapshot_id)
    
    else:
        next_snapshot_result = spark.sql(f"""
            SELECT min(snapshot_id) as next_id 
            FROM {catalog}.{bronze_schema}.part 
            WHERE snapshot_id > '{latest_snapshot_version}'
        """).collect()[0]
        
        if next_snapshot_result.next_id is None:
            return None
        
        next_snapshot_id = next_snapshot_result.next_id
        df = spark.sql(f"""
            SELECT * FROM {catalog}.{bronze_schema}.part 
            WHERE snapshot_id = '{next_snapshot_id}'
        """)
        
        return (df, next_snapshot_id)