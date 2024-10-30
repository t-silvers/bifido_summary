attach 'data_lake/indexes/abundance.duckdb' as adb (readonly);

attach 'data_lake/indexes/samples.duckdb' as sdb (readonly);

create table reference_genomes as
with obs_taxon as (
    select *
        , regexp_replace(
            trim(abundance.name), ' ', '_', 'g'
        ) as taxon_raw
    from abundance 
    inner join (
        select 
            "sample"
            , max(fraction_total_reads) as max_frac 
        from abundance 
        where "name" != 'Homo sapiens'
        group by "sample"
    ) obs_max 
    on 
        abundance.sample = obs_max.sample 
    and abundance.fraction_total_reads = obs_max.max_frac 
)
select 
    "sample"
    , try_cast(
        coalesce(
            taxon_ref.taxon, obs_taxon.taxon_raw
        ) as bacteria_taxon
    ) as taxon
from obs_taxon
left join taxon_ref
on taxon_ref.taxon_raw = obs_taxon.taxon_raw;