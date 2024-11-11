copy (

    select 
        sample_info.sample
        , columns('fastq_[1,2]')

    from
        read_csv(getenv('FASTQS')) fastqs
        , sample_info

    where
        sample_info.ID = fastqs.ID
    order by
        sample_info.sample

) to '/dev/stdout' (format csv);