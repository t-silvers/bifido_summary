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
        'results/raw_sample_info.xlsx'
    output:
        'results/samplesheet.csv'
    localrule: True
    run:
        import babybiome_samplesheet_utils as bbb
        import pandas as pd
        
        # NOTE: Sheet names learned manually before ingestion:
        # >>> from openpyxl import Workbook
        # >>> openpyxl.reader.excel.load_workbook(input[0]).sheetnames
        # ['LibraryPlate1-3', 'LibraryPlate4-8']

        read_kwargs = {
            'LibraryPlate1-3': {
                'sheet_name': 'LibraryPlate1-3',
                'engine': 'openpyxl',
                'names': ['ID', 'species', 'relationship-time']
            },
            'LibraryPlate4-8': {
                'sheet_name': 'LibraryPlate4-8',
                'engine': 'openpyxl',
                'header': None,
                'names': ['ID', 'gDNA', 'species', 'species_extra', '', 'relationship', 'time_cat']
            },
        }

        time_key = {
            'vor': -2,
            '2Wochen': 2,
            '4Wochen': 4,
            '2Monate': 8,
            '3Monate': 13,
            '6Monate': 26,
        }

        (
            pd.concat([
                pd.read_excel(input[0], **read_kwargs['LibraryPlate1-3'])
                .assign(
                    relationship=lambda df: df['relationship-time'].str.extract(r'(\w+)-'),
                    time_cat=lambda df: df.pop('relationship-time').str.extract(r'-(\w+)'),
                ),
                pd.read_excel(input[0], **read_kwargs['LibraryPlate4-8'])
            ], axis=0)
            .assign(
                donor=lambda df: bbb.utils.extract_donor_id(df['ID']),
                ID=lambda df: df.pop('ID').replace(regex={'_': '-'}),
                time_weeks=lambda df: df['time_cat'].replace(time_key),
            )
            .pipe(bbb.create_samplesheet, directory='/ptmp/thosi/datasets/230119_B001_Lib/data')

            # Create unique identifier, `sample`
            .rename_axis('sample')
            .reset_index()
            .dropna(subset=['fastq_1', 'fastq_2'])

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