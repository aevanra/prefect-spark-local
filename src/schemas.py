from pyspark.sql import types


genres = [
    "unknown",
    "action",
    "adventure",
    "animation",
    "children",
    "comedy",
    "crime",
    "documentary",
    "drama",
    "fantasy",
    "film_noir",
    "horror",
    "musical",
    "mystery",
    "romance",
    "sci_fi",
    "thriller",
    "war",
    "western",
]

schemas = {
    "ratings": types.StructType(
        [
            types.StructField("user_id", types.StringType(), False),
            types.StructField("item_id", types.StringType(), False),
            types.StructField("rating", types.IntegerType(), False),
            types.StructField("timestamp", types.LongType(), False),
        ]
    ),
    "users": types.StructType(
        [
            types.StructField("id", types.StringType(), False),
            types.StructField("age", types.IntegerType(), False),
            types.StructField("gender", types.StringType(), False),
            types.StructField("occupation", types.StringType(), False),
            types.StructField("zip_code", types.StringType(), False),
        ]
    ),
    "movies": types.StructType(
        [
            types.StructField("id", types.StringType(), False),
            types.StructField("title", types.StringType(), False),
            types.StructField("release_date", types.StringType(), True),
            types.StructField("video_release_date", types.StringType(), True),
            types.StructField("imdb_url", types.StringType(), True),
        ] + [types.StructField(genre, types.IntegerType(), True) for genre in genres]
    ),
}
