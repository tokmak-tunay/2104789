CREATE TABLE tokmaktunay_homework.silver_views
    WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://tokmak.tunay/datalake/views_silver/'
    ) AS SELECT article,views,rank, date
         FROM tokmaktunay_homework.bronze_views;
         
