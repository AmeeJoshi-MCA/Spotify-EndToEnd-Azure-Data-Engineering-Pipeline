import dlt

expectation = {
    "rule_1" : "user_id IS NOT NULL"
}


# 1. Define the Staging Table
@dlt.table(name="DimUserStaging")
@dlt.expect_all_or_drop(expectation)
def DimUserStaging():
    # Backticks are essential for hyphenated names like `spotify-silver`
    return (
        spark.readStream
       .option("ignoreChanges", "true") # <--- THIS IS THE KEY
        .table("spotify_catalog.`spotify-silver`.dimuser")
    )

# 2. DECLARE the Target Table (This fixes the "Not Defined" error)
dlt.create_streaming_table("DimUser")

# 3. Apply the CDC Changes
dlt.apply_changes(
    target = "DimUser",
    source = "DimUserStaging",
    keys = ["user_id"],
    sequence_by = "updated_at",
    stored_as_scd_type = 1
)
 