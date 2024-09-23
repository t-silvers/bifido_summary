set memory_limit = getenv('MEMORY_LIMIT');
set threads = getenv('SLURM_CPUS_PER_TASK');

create table abundance as
select 
    * exclude("filename")
    , regexp_extract(
        "filename",
        'bracken/(\d+).bracken',
        1
    )
from read_csv(
    getenv('BRACKEN_GLOB'),
    -- '/ptmp/thosi/bifido_summary/results/bracken/*.bracken',
	auto_detect = false,
	header = true,
	delim = '\t',
	columns = {
		'name': 'varchar',
		'taxonomy_id': 'varchar',
		'taxonomy_lvl': 'varchar',
		'kraken_assigned_reads': 'ubigint',
		'added_reads': 'ubigint',
		'new_est_reads': 'ubigint',
		'fraction_total_reads': 'float4'
	},
    filename = true
);

-- TODO: Add metadata on calling, code, etc.