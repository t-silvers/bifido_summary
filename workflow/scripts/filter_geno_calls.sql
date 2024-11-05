set memory_limit = getenv('MEMORY_LIMIT');
set threads = getenv('SLURM_CPUS_PER_TASK');

-- export GLOB="data_lake/variants/*/*/*/*/data.parquet"
-- export DP=8 MAF=.95 QUAL=30 SPECIES=Bifidobacterium_longum STRAND_DP=3

select * from read_parquet(
    'data_lake/vcfs/*/*/*/*/*.parquet',
    hive_partitioning = true
);

-- -----------

create temp table test06 as
select *
from read_parquet(
    'data_lake/variants/*/*/*/*/data.parquet',
    hive_partitioning = true,
    hive_types = {'species': varchar, 'sample': int, 'INDEL': int, 'DP': int}
)
where species = getenv('SPECIES')
    and chromosome = 'NC_015067.1'
    and "position" > 1741590
    and "position" < 1741610
    and "sample" = 297
order by "position";

SELECT column_name
FROM information_schema.columns
WHERE table_name = 'your_table_name';

PRAGMA show_columns('your_table_name');

PRAGMA table_info('your_table_name');
CALL pragma_table_info('your_table_name');

create temp table test03 as
select 
    "sample"
    , chromosome
    , "position"
    , reference
    , coalesce(nullif(alternate[1], ''), reference) as allele 
    , info_DP4
from read_parquet(
    'data_lake/variants/*/*/*/*/data.parquet',
    hive_partitioning = true,
    hive_types = {'species': varchar, 'sample': int, 'INDEL': int, 'DP': int}
)
where species = getenv('SPECIES')
    -- and INDEL = 0
    and chromosome = 'NC_015067.1'
    and "position" = 1741599;

select 
    chromosome, "position", reference, alternate, info_DP, info_DP4, info_ADF, info_ADR, format_297_SP
from read_parquet(
    'data_lake/variants/species=Bifidobacterium_longum/sample=297/INDEL=0/*/data.parquet'
)
where format_297_SP = 0
order by "position";

select 
    chromosome, "position", reference, alternate, info_DP, info_DP4, info_ADF, info_ADR, format_297_SP
from read_parquet(
    'data_lake/variants/species=Bifidobacterium_longum/sample=297/INDEL=0/*/data.parquet'
)
where format_297_SP > 0
-- order by "position";
order by format_297_SP;

-- 626960
-- 626824

-- then filter
create temp table filtered_calls as
with pseudogenome as (
    select 
        "sample"
        , chromosome
        , "position"
        , reference
        , coalesce(nullif(alternate[1], ''), reference) as allele 
    from read_parquet(
        'data_lake/variants/*/*/*/*/data.parquet',
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
)
select g.* 
from variable_sites v
inner join pseudogenome g
        on v.chromosome = g.chromosome
       and v.position = g.position;

-- remove bad samples
select "sample", count(*) as n from filtered_calls group by "sample" having n > 1000 order by n;

select chromosome, "position", count(*) as n from filtered_calls group by chromosome, "position" order by n;

create temp table test01 as
select * from filtered_calls
where "sample" in (
    select "sample" from filtered_calls group by "sample" having count(*) > 6000
);

select "sample", count(*) as n from test01 group by "sample" order by n;

select chromosome, "position", count(*) as n from test01 group by chromosome, "position" order by n;

select chromosome, "position", count(*) as n from test01 group by chromosome, "position" having n > 42 order by n;



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
    -- NOTE: Above median
    --    and count(distinct "sample") > (select count(distinct "sample") / 2 from pseudogenome)
    -- NOTE: Non-singleton
       and count(distinct "sample") > 1
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