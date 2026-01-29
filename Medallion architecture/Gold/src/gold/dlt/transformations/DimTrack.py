import dlt

# 1. Define the Staging Table
@dlt.table(name="DimTrackStaging")
def DimTrackStaging():
    # Backticks are essential for hyphenated names like `spotify-silver`
     return (
        spark.readStream
       .option("ignoreChanges", "true") # <--- THIS IS THE KEY
        .table("spotify_catalog.`spotify-silver`.dimtrack")
    )

# 2. DECLARE the Target Table (This fixes the "Not Defined" error)
dlt.create_streaming_table("DimTrack")

# 3. Apply the CDC Changes
dlt.apply_changes(
    target = "DimTrack",
    source = "DimTrackStaging",
    keys = ["track_id"],
    sequence_by = "updated_at",
    stored_as_scd_type = 1
)
 