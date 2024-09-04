checkpoint mapping_samplesheet:
    input:
        'results/samplesheet.csv'
    output:
        'results/{species}/samplesheet.csv'
    localrule: True
    run:
        import pandas as pd

        samplesheet = pd.read_csv(input[0])

        if wildcards.species == 'Control':
            species_mask = samplesheet['species'].isna()
        else:
            species_mask = samplesheet['species'] == species_key.get(wildcards.species)

        (
            samplesheet[species_mask]
            .filter(['sample', 'fastq_1', 'fastq_2'])
            .to_csv(output[0], index=False)
        )


use rule bactmap from widevariant as mapping with:
    input:
        input='results/{species}/samplesheet.csv',
        reference=lambda wildcards: config['reference'][wildcards.species],
    params:
        pipeline='bactmap',
        profile='singularity',
        nxf='-work-dir results/{species}/work -config config/bactmap.config',
        outdir='results/{species}',
    output:
        'results/{species}/multiqc/multiqc_data/multiqc_fastp.yaml',
        'results/{species}/multiqc/multiqc_data/mqc_bcftools_stats_vqc_Count_SNP.yaml',
    localrule: True
    envmodules:
        'apptainer/1.3.2',
        'nextflow/21.10',
        'jdk/17.0.10'