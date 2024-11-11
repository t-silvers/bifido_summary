copy (
    select
        regexp_replace(seq_info.ID, '_', '-') as ID
        , columns('fastq_[1,2]')
    from (
        select
            *
        from
            read_csv(getenv('FASTQS'))
    ) fastqs, seq_info
    where
        regexp_replace(fastqs.ID, '-', '_') = seq_info.original_id
) to '/dev/stdout' (format csv);