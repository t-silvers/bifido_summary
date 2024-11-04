# TODO: Replace with SQL script
# TODO: Paritioning on DP mislabeled
rule hive_partition_vcfs:
    input:
        ancient('results/{species}/variants/{sample}.vcf.gz'),
    output:
        'data_lake/logs/vcfs.species={species}.sample={sample}.done'
    params:
        prefix='data_lake/variants/species={species}/sample={sample}'
    resources:
        cpus_per_task=4,
        runtime=5
    envmodules:
        'bcftools/1.20',
        'vcf2parquet/0.4.1'
    shell:
        '''
        for indel in 0 1; do
          for dp in 2 3 4 5 6 7; do
            output="{params.prefix}/INDEL=$indel/DP=$dp/vcf.parquet"

            mkdir -p $(dirname $output)
            
            bcftools view -i 'INFO/INDEL='"$indel"' & (SUM(INFO/ADF)<'"$((dp+1))"' | SUM(INFO/ADR)<'"$((dp+1))"') & SUM(INFO/ADF)>='"$dp"' & SUM(INFO/ADR)>='"$dp" {input} |\
              vcf2parquet -i /dev/stdin convert -o "{params.prefix}/INDEL=$indel/DP=$dp/data.parquet"
          done
        done

        touch {output} # Not source why using `touch()` in output not working ...
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
        'data_lake/logs/vcfs.species={{species}}.sample={sample}.done',
        sample=sample_ids
    )


rule:
    input:
        candidate_variant_tables,
        samples_db='data_lake/indexes/samples.duckdb',
    output:
        'results/{species}/annot_filtered_calls.csv',
    params:
        glob="'data_lake/variants/*/*/*/*/data.parquet'",
        strand_dp=config['variants_thresh']['strand_dp'],
        dp=config['variants_thresh']['dp'],
        maf=config['variants_thresh']['maf'],
        qual=config['variants_thresh']['qual'],
    resources:
        cpus_per_task=32,
        mem_mb=96_000,
        runtime=15,
    envmodules:
        'duckdb/nightly'
    shell:
        '''
        export MEMORY_LIMIT="$(({resources.mem_mb} / 1200))GB"
        export GLOB={params.glob}
        export DP={params.dp} MAF={params.maf} QUAL={params.qual} SPECIES={wildcards.species} STRAND_DP={params.strand_dp}

        duckdb -readonly -init config/.duckdbrc {input.samples_db} \
          -c ".read workflow/scripts/filter_geno_calls.sql" > {output}
        '''