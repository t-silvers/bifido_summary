def paired_fastqs(wildcards):
    import pandas as pd

    print(pd.read_csv(
            checkpoints.samplesheet.get(**wildcards).output[0]
        ))

    print(pd.read_csv(
            checkpoints.samplesheet.get(**wildcards).output[0]
        ).dtypes)

    fastqs = (
        pd.read_csv(
            checkpoints.samplesheet.get(**wildcards).output[0]
        )
        .query(
            f"sample == '{wildcards['sample']}'"
        )
        .filter(like='fastq', axis=1)
        .values
        .flatten()
    )

    print(fastqs)
    
    return fastqs


rule kraken2:
    input:
        paired_fastqs
    output:
        'results/kraken2/{sample}.kraken2',
        'results/kraken2/{sample}.k2report',
    params:
        db=config['public_data']['kraken_db'],
    resources:
        cpus_per_task=4,
        mem_mb=96_000,
        runtime=10,
    localrule: True
    envmodules:
        'kraken2/2.1.3'
    shell:
        '''
        kraken2 \
          --db {params.db} \
          --threads {resources.cpus_per_task} \
          --report-minimizer-data \
          --paired \
          --minimum-hit-groups 3 \
          --output {output[0]} \
          --report {output[1]} \
          {input}
        '''


rule bracken:
    input:
        'results/kraken2/{sample}.k2report',
    output:
        'results/bracken/{sample}.bracken',
        'results/bracken/{sample}.breport',
    params:
        db=config['public_data']['kraken_db'],
    resources:
        runtime=5,
    envmodules:
        'bracken/2.9'
    shell:
        'bracken -d {params.db} -r 100 -i {input} -o {output[0]} -w {output[1]}'


def species_abundance_output(wildcards):
    import pandas as pd

    sample_ids = (
        pd.read_csv(
            checkpoints.samplesheet.get(
                **wildcards
            ).output[0]
        )
        ['sample'].astype(str)
    )

    return expand(
        [
            'results/bracken/{sample}.bracken',
            'results/bracken/{sample}.breport',
        ],
        sample=sample_ids
    )


rule:
    input:
        species_abundance_output
    output:
        touch('results/abundance.done')
    localrule: True