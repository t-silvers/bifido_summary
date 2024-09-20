def paired_fastqs(wildcards):
    import pandas as pd

    return (
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


rule kraken2:
    """Analysis using Kraken2.

    Returns:
        Kraken Report File: (tab-delimited columns)
          - Percentage of reads for the subtree rooted at this taxon
          - Total number of reads for the subtree rooted at this taxon
          - Number of reads assigned directly to this taxon level
          - Taxonomical Rank [U, -, D, P, C, O, F, G, S]
          - NCBI Taxonomy ID
          - Scientific Name - Indented (2 spaces per level)

    References:
        - https://ccb.jhu.edu/software/bracken/index.shtml?t=manual#format
        - https://ccb.jhu.edu/software/kraken/dl/README
    """
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
    """Abundance estimation using Bracken.

    Args:
        Kraken Report File: (bracken input file; tab-delimited columns) (see rule kraken2)

    Returns:
        Bracken-Recalculated Kraken Report File: (bracken input file; tab-delimited columns) (see rule kraken2)

          For input kraken2 report `/path/to/file.kreport`, Bracken generates 
          `/path/to/file_bracken_species.kreport`, and appears constrained to match the 
          input file pattern.

        Bracken Output File Format: (bracken output file; tab-delimited columns)
          - Name
          - Taxonomy ID
          - Level ID (S=Species, G=Genus, O=Order, F=Family, P=Phylum, K=Kingdom)
          - Kraken Assigned Reads
          - Added Reads with Abundance Reestimation
          - Total Reads after Abundance Reestimation
          - Fraction of Total Reads

    References:
        https://ccb.jhu.edu/software/bracken/index.shtml?t=manual#step3
    """
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