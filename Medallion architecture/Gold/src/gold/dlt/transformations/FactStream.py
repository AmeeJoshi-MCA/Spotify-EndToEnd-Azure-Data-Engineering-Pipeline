import dlt

# 1. Define the Staging Table
@dlt.table(name="FactStreamStaging")
def FactStreamStaging():
    # Backticks are essential for hyphenated names like `spotify-silver`
   return (
        spark.readStream
       .option("ignoreChanges", "true") # <--- THIS IS THE KEY
        .table("spotify_catalog.`spotify-silver`.factstream")
    )

# 2. DECLARE the Target Table (This fixes the "Not Defined" error)
dlt.create_streaming_table("FactStream")

# 3. Apply the CDC Changes
dlt.apply_changes(
    target = "FactStream",
    source = "FactStreamStaging",
    keys = ["stream_id"],
    sequence_by = "stream_timestamp",
    stored_as_scd_type = 1
)
 