rule:
    output:
        'results/raw_seq_info.csv'
    params:
        path=config["samplesheet"]['seq_info']
    resources:
        slurm_partition='datatransfer'
    localrule: True
    envmodules:
        'rclone/1.67.0'
    shell:
        'rclone copyto "nextcloud:{params.path}" {output}'


rule:
    output:
        'results/raw_sample_info.xlsx'
    params:
        path=config["samplesheet"]['sample_info']
    resources:
        slurm_partition='datatransfer'
    localrule: True
    envmodules:
        'rclone/1.67.0'
    shell:
        'rclone copyto "nextcloud:{params.path}" {output}'


checkpoint samplesheet:
    input:
        'results/raw_sample_info.xlsx'
    output:
        multiext('results/test_samplesheet', '.duckdb', '.csv')
    localrule: True
    envmodules:
        'duckdb/1.0'
    shell:
        '''
        export MEMORY_LIMIT="8GB" SLURM_CPUS_PER_TASK=2 SAMPLESHEET="{input}"
        duckdb {output[0]} -c ".read workflow/scripts/create_samplesheet_db.sql"
        duckdb {output[0]} -s ".mode csv; copy samplesheet to '/dev/stdout';" > {output[1]}
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