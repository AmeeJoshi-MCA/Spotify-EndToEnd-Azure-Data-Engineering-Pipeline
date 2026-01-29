class BronzeReader:
    def __init__(self, spark):
        self.spark = spark

    def read_bronze_stream(self, format_type, schema_path, input_path):
        """
        Reads streaming data using Auto Loader (cloudFiles).
        """
        return self.spark.readStream.format("cloudFiles") \
            .option("cloudFiles.format", format_type) \
            .option("cloudFiles.schemaLocation", schema_path) \
            .load(input_path)