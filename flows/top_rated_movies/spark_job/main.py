if __name__ == "__main__":
    from pyspark.sql.window import Window
    import pyspark.sql.functions as F

    from src.auth import get_aws_authenticated_session
    from src.constants import S3A_WORK_DIR
    from src.load_dataframes import load_movies_data, load_ratings_data
    from src.logger import get_logger
    from src.schemas import genres
    import sys

    logger = get_logger(__name__)

    # Set config options here
    try:
        min_reviews = int(sys.argv[1])
        precision = int(sys.argv[2])
    except IndexError:
        logger.error("Passed parameters were invalid")
        logger.error("Usage `python main.py <min_reviews: int> <precision: int>`")
        logger.error("Exiting...")
        sys.exit(1)


    # Init session
    spark = get_aws_authenticated_session("TopMovieByGenre")

    # Grab data
    ratings, movies = load_ratings_data(spark), load_movies_data(spark)


    # Calculate average ratings for movies
    average_ratings = ratings.groupBy("item_id").agg(
        F.avg("rating").alias("avg_rating"), F.count("rating").alias("count_rating")
    )

    average_ratings = average_ratings.select("item_id", "avg_rating").filter(
        average_ratings.count_rating >= min_reviews
    )

    # Unpivot to consolidate genre columns
    stack_str = (
        f"stack({len(genres)}, "
        + ", ".join([f"'{genre}', {genre}" for genre in genres])
        + ") as (genre, genre_flag)"
    )

    unpivoted_movies = (
        movies.select("id", "title", F.expr(stack_str))
        .filter(F.col("genre_flag") == 1)
        .select("id", "title", "genre")
    )

    # Smash them together
    movies_with_avg_ratings = average_ratings.join(
        other=unpivoted_movies,
        on=average_ratings.item_id == unpivoted_movies.id,
        how="inner",
    ).select(
        "title",
        "genre",
        "avg_rating",
        F.dense_rank()
        .over(Window.partitionBy("genre").orderBy(F.col("avg_rating").desc()))
        .alias("rank"),
    )

    # grab only the top ones
    top_movies = movies_with_avg_ratings.filter(
        movies_with_avg_ratings.rank == 1
    ).select("title", "genre", F.round("avg_rating", precision).alias("avg_rating"))

    top_movies.show()

    top_movies.write.csv(path=f"{S3A_WORK_DIR}/top_movies_by_genre", mode="overwrite", sep="|", header=True)
    
