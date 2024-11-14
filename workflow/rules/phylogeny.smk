rule:
    input:
        ancient('results/{species}/annot_filtered_calls.csv'),
    output:
        'results/{species}/filtered_pseudogenome.fas',
    params:
        db='data/index.duckdb',
    resources:
        cpus_per_task=12,
        mem_mb=16_000,
        runtime=5,
    envmodules:
        'duckdb/nightly'
    shell:
        '''
        export MEMORY_LIMIT="$(({resources.mem_mb} / 1200))GB"
        export FILTERED_CALLS={input}

        duckdb -readonly {params.db} -c ".read workflow/scripts/calls_to_msa.sql" > {output}
        '''


rule filter_invariant_sites:
    input:
        'results/{species}/filtered_pseudogenome.fas',
    output:
        'results/{species}/filtered_pseudogenome-filtered.fas',
    localrule: True
    envmodules:
        'snp-sites/2.5.1'
    shell:
        '''
        snp-sites -o {output} {input}
        '''


rule raxml_ng:
    input:
        'results/{species}/filtered_pseudogenome-filtered.fas',
    params:
        extra='--all --model GTR+G --bs-trees 200',
        # outgroup=lambda wildcards: '--outgroup ' + config['outgroup'][wildcards.donor][wildcards.species]['ID'],
        outgroup='',
        prefix='results/{species}/{species}',
    output:
        multiext(
            'results/{species}/{species}.raxml',
            '.reduced.phy',
            '.rba',
            '.bestTreeCollapsed',
            '.bestTree',
            '.mlTrees',
            '.support',
            '.bestModel',
            '.bootstraps',
            '.log'
        )
    resources:
        cpus_per_task=48,
        mem_mb=64_000,
        runtime=120,
    envmodules:
        'raxml-ng/1.2.2_MPI'
    shell:
        '''
        export OMP_PLACES=threads
        raxml-ng \
          {params.extra} \
          {params.outgroup} \
          --msa {input} \
          --threads {resources.cpus_per_task} \
          --prefix {params.prefix} \
          --redo
        touch {output}
        '''


# def trees_output(wildcards):
#     import pandas as pd

#     species_list = config['wildcards']['species'].split('|')

#     matched_donors = (
#         pd.read_csv(
#             checkpoints.samplesheet.get(
#                 **wildcards
#             ).output[0]
#         )
#         .query('donor == @donor_list')
#         .filter(['sample', 'donor'])
#         .drop_duplicates()
#     )
    
#     matched_species = (
#         pd.read_csv(
#             checkpoints.reference_genomes.get(
#                 **wildcards
#             ).output[1]
#         )
#         .rename(columns={'taxon': 'species'})
#         .query('species == @species_list')
#         .filter(['sample', 'species'])
#         .drop_duplicates()
#     )

#     return list(
#         matched_donors
#         .merge(matched_species)
#         .assign(sample=lambda df: df.pop('sample').astype(int))
#         .query('sample != @EXCLUDE')
#         .query('sample != @INDEX_SWAPPED')
#         .drop_duplicates()
#         .transpose()
#         .apply(lambda df: 'results/raxml_ng/FAMILY={donor}/SPECIES={species}.raxml.bestTree'.format(**df.to_dict()))
#         .values
#         .flatten()
#     )


# rule:
#     input:
#         trees_output
#     output:
#         touch('results/phylogenies.done')
#     localrule: True