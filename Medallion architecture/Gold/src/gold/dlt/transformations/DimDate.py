import dlt

# 1. Define the Staging Table
@dlt.table(name="DimDateStaging")
def DimDateStaging():
    # Backticks are essential for hyphenated names like `spotify-silver`
    return (
        spark.readStream
       .option("ignoreChanges", "true") # <--- THIS IS THE KEY
        .table("spotify_catalog.`spotify-silver`.dimdate")
    )

# 2. DECLARE the Target Table (This fixes the "Not Defined" error)
dlt.create_streaming_table("DimDate")

# 3. Apply the CDC Changes
dlt.apply_changes(
    target = "DimDate",
    source = "DimDateStaging",
    keys = ["date_key"],
    sequence_by = "date",
    stored_as_scd_type = 1
)
 