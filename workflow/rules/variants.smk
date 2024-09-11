rule:
    input:
        'results/{species}/variants/{sample}.vcf.gz',
    output:
        temp('results/{species}/variants/{sample}_af.tsv')
    resources:
        cpus_per_task=2,
        runtime=5
    localrule: False
    envmodules:
        'bcftools/1.20'
    shell:
        '''
        bcftools +fill-tags {input} -- -t AF | \
        bcftools query -f '%CHROM\t%POS\t%REF\t%ALT[\t%SAMPLE]\t%QUAL\t%DP\t%INFO/DP4\n' > {output}
        '''


def candidate_variant_tables(wildcards):
    import pandas as pd

    mapping_samplesheet_path = checkpoints.mapping_samplesheet.get(
        species=wildcards.species,
    ).output[0]
    
    sample_ids = pd.read_csv(mapping_samplesheet_path)['sample'].astype(str)

    return expand(
        'results/{{species}}/variants/{sample}_af.tsv',
        sample=sample_ids,
    )


rule:
    input:
        candidate_variant_tables
    params:
        af_glob="'results/{species}/variants/*_af.tsv'",
    output:
        'results/{species}/variants/candidate_variants.duckdb',
    resources:
        cpus_per_task=32,
        mem_mb=48_000,
        runtime=15
    localrule: False
    envmodules:
        'duckdb/nightly'
    shell:
        '''
        export MEMORY_LIMIT="$(({resources.mem_mb} / 1000))GB" \
               BCFTOOLS_QUERY={params.af_glob}
        duckdb {output} -c ".read workflow/scripts/create_variants_db.sql"
        '''