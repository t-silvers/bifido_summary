set memory_limit = getenv('MEMORY_LIMIT');
set threads = getenv('SLURM_CPUS_PER_TASK');

-- Filter calls
create temp table filtered_calls as
with pseudogenome as (
    select 
        "sample"
        , chromosome
        , "position"
        , reference
        , coalesce(nullif(alternate[1], ''), reference) as allele 
    from read_parquet(
        getenv('GLOB'),
        hive_partitioning = true,
        hive_types = {'species': varchar, 'sample': int, 'INDEL': int, 'DP': int}
    )
    where species = getenv('SPECIES')
      -- NOTE: Removes indels
      and INDEL = 0
      and DP >= cast(getenv('STRAND_DP') as int)
      and quality > cast(getenv('QUAL') as float)
      and array_reduce(info_DP4, (x, y) -> x + y) >= cast(getenv('DP') as int)
      and list_aggregate(info_ADF, 'max') >= cast(getenv('STRAND_DP') as int)
      and list_aggregate(info_ADR, 'max') >= cast(getenv('STRAND_DP') as int)
      and abs(array_reduce(info_DP4[3:4], (x, y) -> x + y) / array_reduce(info_DP4, (x, y) -> x + y) - .5) >= (cast(getenv('MAF') as float) / 2)
),
variable_sites as (
	select chromosome, "position" 
    from pseudogenome
	group by chromosome, "position"
	having count(distinct allele) > 1
       and count(distinct "sample") > (select count(distinct "sample") / 2 from pseudogenome)
)
select g.* 
from variable_sites v
inner join pseudogenome g
        on v.chromosome = g.chromosome
       and v.position = g.position;

-- Annotate filtered calls
create temp table annot_calls as
select 
    a.ID
    , a.donor
    , a.relationship
    , a.time_weeks
    , g.chromosome
    , g.position
    , g.reference
    , g.allele
from samples a
inner join filtered_calls g
        on a.sample = g.sample;

copy (select * from annot_calls) to '/dev/stdout' (format csv);