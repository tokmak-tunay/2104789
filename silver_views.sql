CREATE EXTERNAL TABLE
tunay.bronze_edits (
    title STRING,
    edits INT,
    rank INT,
    date DATE,
    retrieved_at TIMESTAMP) 
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://tokmak.tunay/datalake/views/';
 