species_key = {
    'Ecoli': 'Escherichia coli',
    'Bifido': 'Bifidobacterium spp',
    'Bovatus': 'Bacteroides ovatus/xylanisolvens',
    'Efaecalis': 'Enterococcus faecalis',
    'Koxytoca': 'Klebsiella oxytoca',
    'Control': None
}


rule:
    output:
        'results/raw_seq_info.csv'
    params:
        url=config['seq_info_url']
    resources:
        slurm_partition='datatransfer'
    localrule: True
    shell:
        'wget -nc -O {output} {params.url}'


rule:
    output:
        'results/raw_sample_info.xlsx'
    params:
        url=config['sample_info_url']
    resources:
        slurm_partition='datatransfer'
    localrule: True
    shell:
        'wget -nc -O {output} {params.url}'


checkpoint samplesheet:
    input:
        ancient('results/raw_sample_info.xlsx')
    output:
        'results/samplesheet.csv'
    localrule: True
    run:
        import babybiome_samplesheet_utils as bbb
        import pandas as pd

        read_kwargs = {
            'engine': 'openpyxl',
            'names': ['ID', 'species', 'relationship']
        }

        (
            pd.read_excel(input[0], **read_kwargs)
            .assign(
                donor=lambda df: bbb.utils.extract_donor_id(df['ID']),
                ID=lambda df: df['ID'].replace(regex={'_': '-'}),
            )
            .pipe(bbb.create_samplesheet, directory=config['data']['directory'])

            # Create unique identifier, `sample`
            .rename_axis('sample')
            .reset_index()
            .dropna()
            .pipe(bbb.SamplesheetSchema.validate)
            .to_csv(output[0], index=False)
        )


rule:
    input:
        expand(
            'results/{info}.csv',
            info=['raw_seq_info', 'samplesheet']
        )
    output:
        touch('results/samplesheet.done')
    localrule: True