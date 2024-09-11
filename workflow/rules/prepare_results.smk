rule record_samples:
    input:
        'results/samplesheet.csv'
    output:
        'results/samplesheet.duckdb',
    resources:
        cpus_per_task=1,
        mem_mb=1_024
    localrule: True
    envmodules:
        'duckdb/nightly'
    shell:
        '''
        duckdb {output} -s \
          "set memory_limit = '$(({resources.mem_mb} / 1000))GB';
          set threads = {resources.cpus_per_task};
          create table samplesheet as 
          select * 
          from read_csv('{input}');"
        '''