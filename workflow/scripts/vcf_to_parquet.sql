set memory_limit = getenv('MEMORY_LIMIT');
set threads = getenv('SLURM_CPUS_PER_TASK');

create temp table parsed_vcf as
with vcf as (
    select * exclude (FORMAT)
        , try_cast(
            regexp_extract(
                FORMATvals,
                '(\d+):(\d+):(\d+):(\d+):(\d+):(\d+)',
                ['GT', 'DP', 'SP', 'ADF', 'ADR', 'AD']
            ) as struct(
                "GT" int, "DP" int, "SP" int, "ADF" int, "ADR" int, "AD" int
            )
        ) as FORMAT
        , case when INFO ilike '%INDEL%' then true else false end as "indel"
        , getenv('SPECIES') as species
        , getenv('SAMPLEID') as "sample"
    from read_csv(
        getenv('VCF'),
        compression = 'gzip',
        delim = '\t',
        names = ['#CHROM', 'POS', 'ID', 'REF', 'ALT', 'QUAL', 'FILTER', 'INFO', 'FORMAT', 'FORMATvals'],
        nullstr = '.'
    )
)
select 
    species
    , "sample"
    , "#CHROM" as chromosome
    , POS as position
    , REF as reference
    , ALT as alternate
    , QUAL
    , FORMAT.*
    , "indel"
from vcf;

copy parsed_vcf to 'data_lake/vcfs' (
    format parquet, 
    partition_by (species, "sample", chromosome, "indel"), 
    overwrite_or_ignore true
);