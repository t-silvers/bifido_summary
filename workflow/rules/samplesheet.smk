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


# NOTE: Not all FASTQ files in directory have info in sample sheet
checkpoint samplesheet:
    input:
        'results/raw_sample_info.xlsx'
    output:
        'results/samplesheet.csv'
    localrule: True
    run:
        import babybiome_samplesheet_utils as bbb
        import pandas as pd

        read_kwargs = {
            'engine': 'openpyxl',
            'names': ['ID', 'species', 'relationship-time']
        }

        (
            pd.read_excel(input[0], **read_kwargs)
            .assign(
                donor=lambda df: bbb.utils.extract_donor_id(df['ID']),
                ID=lambda df: df.pop('ID').replace(regex={'_': '-'}),
                relationship=lambda df: df['relationship-time'].str.extract(r'(\w+)-'),
                time_cat=lambda df: df.pop('relationship-time').str.extract(r'-(\w+)'),
                time_weeks=lambda df: df['time_cat'].replace({'vor': -2, '2Wochen': 2, '4Wochen':4}),
            )
            .pipe(bbb.create_samplesheet, directory=config['data']['directory'])

            # Create unique identifier, `sample`
            .rename_axis('sample')
            .reset_index()
            .dropna()

            .assign(species=lambda df: df.pop('species').str.replace(' ', '_'))
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