def species_vcfs(wildcards):
    import pandas as pd

    sample_ids = (
        pd.read_csv(
            checkpoints.mapping_samplesheet.get(
                species=wildcards.species
            ).output[0]
        )
        ['sample']
    )

    return expand(
        'results/{{species}}/variants/{sample}.vcf.gz',
        sample=sample_ids
    )


rule:
    input:
        ancient(species_vcfs)
    output:
        'data/variants/{species}.duckdb'
    params:
        glob="'results/{species}/variants/*.filtered.vcf.gz'"
    resources:
        cpus_per_task=24,
        mem_mb=120_000,
        runtime=15
    envmodules:
        'duckdb/nightly'
    shell:
        '''
        export MEMORY_LIMIT="$(({resources.mem_mb} / 1200))GB"
        export FILTERED_VCFS={params.glob}

        duckdb {output} -c ".read models/annotated_vcfs.sql"
        '''


rule:
    input:
        'data/variants/{species}.duckdb',
    output:
        'results/{species}/annot_filtered_calls.csv',
    params:
        strand_dp=config['variants']['strand_dp'],
        dp=config['variants']['dp'],
        maf=config['variants']['maf'],
        qual=config['variants']['qual'],
    resources:
        cpus_per_task=32,
        mem_mb=96_000,
        runtime=15,
    envmodules:
        'duckdb/nightly'
    shell:
        '''
        export MEMORY_LIMIT="$(({resources.mem_mb} / 1200))GB"
        export DP={params.dp} MAF={params.maf} QUAL={params.qual} STRAND_DP={params.strand_dp}

        duckdb -readonly -init config/.duckdbrc {input} -c ".read workflow/scripts/filter_geno.sql" > {output}
        '''