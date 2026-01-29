class SilverWriter:
    def __init__(self, base_path):
        self.base_path = base_path

    def upsert_to_delta(self, microBatchDF, batchId, table_name, merge_column):
        spark = microBatchDF.sparkSession
        target_path = f"{self.base_path}/{table_name}/data"
        
        # Check if the folder exists instead of the catalog
        try:
            dbutils.fs.ls(target_path)
            folder_exists = True
        except:
            folder_exists = False

        if not folder_exists:
            # First time: Create the folder and data
            microBatchDF.write.format("delta").mode("overwrite").save(target_path)
        else:
            # Subsequent times: MERGE based on the files at the path
            microBatchDF.createOrReplaceTempView("updates")
            
            # Load the existing data as a temp view to perform the merge
            spark.read.format("delta").load(target_path).createOrReplaceTempView("target_table")
            
            spark.sql(f"""
                MERGE INTO target_table t
                USING updates s
                ON t.{merge_column} = s.{merge_column}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
            
            # Write the result back to the same path
            spark.table("target_table").write.format("delta").mode("overwrite").save(target_path)

    def write_silver(self, df, table_name, merge_column, checkpoint_path):
        return df.writeStream \
            .foreachBatch(lambda batch_df, batch_id: self.upsert_to_delta(batch_df, batch_id, table_name, merge_column)) \
            .option("checkpointLocation", checkpoint_path) \
            .trigger(once=True) \
            .start()

    def create_catalog_table(self, spark, table_name, catalog="spotify_catalog", schema="`spotify-silver`"):
            """
            Registers the Delta files into the Unity Catalog.
            Uses backticks to handle hyphens in schema names.
            """
            # Use backticks for the schema because of the hyphen
            full_table_name = f"{catalog}.{schema}.{table_name}"
            
            # Define the physical data path
            target_path = f"{self.base_path}/{table_name}/data"
            
            # Run the SQL command to link the files to the Catalog
            spark.sql(f"CREATE TABLE IF NOT EXISTS {full_table_name} USING DELTA LOCATION '{target_path}'")
            
            print(f"Success: Table {full_table_name} is registered in the catalog.")

            