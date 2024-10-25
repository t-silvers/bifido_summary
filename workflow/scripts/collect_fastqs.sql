set memory_limit = getenv('MEMORY_LIMIT');
set threads = getenv('THREADS');

-- NOTE: Controls are dropped.

copy (
    with fastqs as (
        select "file"
            , regexp_extract(
                "file",
                getenv('PAT'),
                ['library', 'ID', 'read']
            ) as info
        from glob(getenv('FASTQS'))
        where "file" not ilike '%plate%'
    )
    select * 
    from (
        pivot (
            select "file"
                , info.* exclude ("read")
                , 'fastq_' || info.read as "read"
            from fastqs
        ) 
        on "read" 
        using first("file") 
        group by library, ID
    )
) to '/dev/stdout' (format parquet);