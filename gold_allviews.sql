CREATE TABLE tokmaktunay_homework.gold_allviews
    WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://tokmak.tunay/datalake/gold_allviews'
    ) AS SELECT
    title as article,
    sum(views) as total_top_view,
    max(rank) as top_rank,
    count(date) as   ranked_days
         FROM tokmaktunay_homework.silver_views 
         GROUP BY title
 