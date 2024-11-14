-- duckdb -readonly /ptmp/thosi/bifido_summary/data/variants/Bifidobacterium_longum.duckdb

set memory_limit = getenv('MEMORY_LIMIT');
set threads = getenv('SLURM_CPUS_PER_TASK');
set enable_progress_bar = false;

select
    *

from
    read_csv(
        '/ptmp/thosi/bifido_summary/results/Bifidobacterium_longum/annot_filtered_calls.csv'
    )
;