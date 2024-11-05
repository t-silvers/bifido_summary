rule:
    input:
        ancient('results/{species}/variants/{sample}.vcf.gz'),
    output:
        'data_lake/vcfs/species={species}/sample={sample}/.done'
    resources:
        cpus_per_task=4,
        mem_mb=4_000,
        runtime=5
    envmodules:
        'duckdb/nightly'
    shell:
        '''
        export MEMORY_LIMIT="$(({resources.mem_mb} / 1200))GB"
        export SAMPLEID="{wildcards.sample}" SPECIES="{wildcards.species}" VCF="{input}"

        duckdb -c ".read workflow/scripts/vcf_to_parquet.sql"

        touch "{output}" # Not source why using `touch()` in output not working ...
        '''


def candidate_variant_tables(wildcards):
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
        'data_lake/vcfs/species={{species}}/sample={sample}/.done',
        sample=sample_ids
    )


rule:
    input: candidate_variant_tables
    output: 'results/{species}/tmp'
    shell: 'touch {output}'


# rule:
#     input:
#         candidate_variant_tables,
#         samples_db='data_lake/indexes/samples.duckdb',
#     output:
#         'results/{species}/annot_filtered_calls.csv',
#     params:
#         glob="'data_lake/variants/*/*/*/*/data.parquet'",
#         strand_dp=config['variants']['strand_dp'],
#         dp=config['variants']['dp'],
#         maf=config['variants']['maf'],
#         qual=config['variants']['qual'],
#     resources:
#         cpus_per_task=32,
#         mem_mb=96_000,
#         runtime=15,
#     envmodules:
#         'duckdb/nightly'
#     shell:
#         '''
#         export MEMORY_LIMIT="$(({resources.mem_mb} / 1200))GB"
#         export GLOB={params.glob}
#         export DP={params.dp} MAF={params.maf} QUAL={params.qual} SPECIES={wildcards.species} STRAND_DP={params.strand_dp}

#         duckdb -readonly -init config/.duckdbrc {input.samples_db} \
#           -c ".read workflow/scripts/filter_geno_calls.sql" > {output}
#         '''