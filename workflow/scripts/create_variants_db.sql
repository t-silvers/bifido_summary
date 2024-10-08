-- NOTE: Will fail when INDELs are included

set enable_progress_bar = true;
set memory_limit = getenv('MEMORY_LIMIT');
set preserve_insertion_order = false;
set threads = getenv('SLURM_CPUS_PER_TASK');

-- Create ENUM types

create type bases as enum ('A', 'C', 'T', 'G', 'N'); -- NOTE: Unused when indels

create type samples as enum (
    select distinct(regexp_extract("file", '/sample=(\d+)/', 1))
	from glob(getenv('VCFS'))
);

-- Parse VCF files

create table variants as
select 
	species
	, "sample"
	, chromosome
	, cast("position" as ubigint) as "position"
	, cast(reference as bases) as reference
	, array_transform(
		alternate, x -> cast(nullif(x, '') as bases)
	) as alternate
	, cast(quality as decimal(4, 1)) as quality
	, array_transform(
		info_ADF, x -> cast(x as usmallint)
	) as info_ADF
	, array_transform(
		info_ADR, x -> cast(x as usmallint)
	) as info_ADR
	, array_transform(
		info_DP4, x -> cast(x as usmallint)
	) as info_DP4
from read_parquet(
	getenv('VCFS'), 
	hive_partitioning = true,
	hive_types = {
		'species': varchar,
		'sample': samples
	}
);

-- Create ENUM types

create type chroms as enum (
    select distinct(chrom) from variants
);

alter table variants alter chrom type chroms;

create type taxon as enum (
    select distinct(species) from variants
);

alter table variants alter species type taxon;

-- TODO: Add metadata on calling, code, etc.