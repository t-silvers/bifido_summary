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


rule:
    output:
        'results/fastqs-indexswapped.csv'
    params:
        glob=f"'{config['data']['directory']}*.fastq.gz'",
        pat=config['data']['pat'],
    localrule: True
    envmodules:
        'duckdb/1.0'
    shell:
        '''
        export GLOB={params.glob} PAT={params.pat}

        duckdb -init config/.duckdbrc -c ".read models/fastqs.sql" > {output}
        '''


rule:
    input:
        sample_info=ancient('results/raw_sample_info.xlsx'),
        seq_info=ancient('results/raw_seq_info.csv'),
    output:
        'data/.enums.done',
        'data/.sample_info.done',
        'data/.seq_info.done',
    params:
        db='data/index.duckdb'
    localrule: True
    envmodules:
        'duckdb/1.0'
    shell:
        '''
        export SAMPLE_INFO="{input.sample_info}"
        export SEQ_INFO="{input.seq_info}"

        duckdb -init config/.duckdbrc {params.db} -c ".read models/enums.sql"
        duckdb -init config/.duckdbrc {params.db} -c ".read models/sample_info.sql"
        duckdb -init config/.duckdbrc {params.db} -c ".read models/seq_info.sql"

        touch {output}
        '''


rule:
    input:
        'data/.enums.done',
        'data/.sample_info.done',
        'data/.seq_info.done',
        fastqs='results/fastqs-indexswapped.csv',
    output:
        'results/fastqs.csv'
    params:
        db='data/index.duckdb'
    localrule: True
    envmodules:
        'duckdb/1.0'
    shell:
        '''
        export FASTQS="{input.fastqs}"

        duckdb -readonly -init config/.duckdbrc {params.db} -c ".read workflow/scripts/fix_index_swap.sql" > {output}
        '''


checkpoint fastqs:
    input:
        'results/fastqs.csv'
    output:
        'results/kraken_bracken_samplesheet.csv'
    params:
        db='data/index.duckdb'
    localrule: True
    envmodules:
        'duckdb/1.0'
    shell:
        '''
        export FASTQS="{input}"

        duckdb -readonly -init config/.duckdbrc {params.db} -c ".read workflow/scripts/create_abundance_samplesheet.sql" > {output}
        '''