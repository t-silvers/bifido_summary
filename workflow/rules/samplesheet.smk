rule:
    output:
        'data_lake/indexes/fastqs.parquet'
    params:
        glob=f"'{config['data']['directory']}*.fastq.gz'"
        pat=config['data']['pat']
    localrule: True
    envmodules:
        'duckdb/1.0'
    shell:
        '''
        export MEMORY_LIMIT=4GB THREADS=1
        export FASTQS={params.glob} PAT={params.pat}

        duckdb -c ".read workflow/scripts/collect_fastqs.sql" > {output}
        '''

# -- select * from read_csv('/ptmp/thosi/bifido_summary/results/raw_seq_info.csv', skip=13) where "Notes (optional)" ilike '%B001%';

for info, exe in zip(['seq_info', 'sample_info'], ['csv', 'xlsx']):
    rule:
        name:
            f'get_{info}'
        output:
            f'results/raw_{info}.{exe}'
        params:
            path=config['samplesheet'][info]
        resources:
            slurm_partition='datatransfer'
        localrule: True
        envmodules:
            'rclone/1.67.0'
        shell:
            'rclone copyto "nextcloud:{params.path}" {output}'

# TODO: re-parse /ptmp/thosi/bifido_summary/results/raw_seq_info.csv

checkpoint samplesheet:
    input:
        'results/raw_sample_info.xlsx'
    output:
        multiext('results/samplesheet', '.duckdb', '.csv')
    params:
        data_glob=f"'{config['data']['directory']}*.fastq.gz'"
    localrule: True
    envmodules:
        'duckdb/1.0'
    shell:
        '''
        export MEMORY_LIMIT="8GB" \
               SLURM_CPUS_PER_TASK=2 \
               FASTQS_DIR={params.data_glob} \
               SAMPLESHEET="{input}"

        duckdb {output[0]} \
            -c ".read workflow/scripts/create_fastqs_db.sql"

        duckdb -init workflow/scripts/create_types.sql {output[0]} \
            -c ".read workflow/scripts/create_sample_info_db.sql"

        duckdb -csv -init workflow/scripts/create_samplesheet_db.sql {output[0]} \
            -c "set enable_progress_bar = false; copy samplesheet to '/dev/stdout';" > {output[1]}
        '''

rule:
    input:
        expand(
            'results/{info}.csv',
            info=['raw_seq_info', 'samplesheet']
        )
    output:
        touch('results/samplesheet.done')
    localrule: True