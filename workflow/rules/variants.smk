rule create_data_lake:
    input:
        ancient('results/{species}/variants/{sample}.vcf.gz'),
    output:
        multiext(
            'results/lake/species={species}/sample={sample}/',
            'indel.parquet',
            'snp.parquet',
            'nonindels.parquet',
        )
    resources:
        cpus_per_task=4,
        runtime=5
    envmodules:
        'bcftools/1.20',
        'vcf2parquet/0.4.1'
    shell:
        '''
        bcftools view -i 'INFO/INDEL=1' {input} |\
          vcf2parquet -i /dev/stdin convert -o {output[0]}

        bcftools view -i 'TYPE="SNP"' {input} |\
          vcf2parquet -i /dev/stdin convert -o {output[1]}

        bcftools view -e 'INFO/INDEL=1' {input} |\
          vcf2parquet -i /dev/stdin convert -o {output[2]}
        '''


def candidate_variant_tables(wildcards):
    import pandas as pd

    ref_genomes = (
        pd.read_csv(
            checkpoints.reference_genomes.get(
                **wildcards
            ).output[1]
        )
        .assign(sample=lambda df: df['sample'].astype(int))
        .query('sample != @EXCLUDE')
        .query('sample != @INDEX_SWAPPED')
    )

    return list(
        ref_genomes
        [ref_genomes['taxon'].isin(config['wildcards']['species'].split('|'))]
        .filter(['sample', 'taxon'])
        .rename(columns={'taxon': 'species'})
        .drop_duplicates()
        .transpose()
        .apply(lambda df: 'results/lake/species={species}/sample={sample}/nonindels.parquet'.format(**df.to_dict()))
        .values
        .flatten()
    )


rule create_variants_db:
    input:
        candidate_variant_tables
    output:
        'results/candidate_variants.duckdb',
    params:
        vcfs="'results/lake/*/*/nonindels.parquet'",
    resources:
        cpus_per_task=32,
        mem_mb=96_000,
        runtime=90
    envmodules:
        'duckdb/nightly'
    shell:
        '''
        export MEMORY_LIMIT="$(({resources.mem_mb} / 1200))GB" \
               VCFS={params.vcfs}
        
        duckdb {output} -c ".read workflow/scripts/create_variants_db.sql"
        '''