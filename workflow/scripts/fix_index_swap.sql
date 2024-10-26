-- export FASTQS="data_lake/indexes/fastqs-indexswapped.parquet"
-- duckdb data_lake/indexes/seq.duckdb

select * from read_parquet(getenv('FASTQS'));