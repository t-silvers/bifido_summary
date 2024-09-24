set memory_limit = getenv('MEMORY_LIMIT');
set threads = getenv('SLURM_CPUS_PER_TASK');

load spatial;

create type family_relationship as enum ('F', 'M', 'B');

create temp table relationship_ref (
    relationship_raw varchar,
    relationship family_relationship
);

insert into relationship_ref (relationship_raw, relationship) 
values 
    ('Vater', 'F'), 
    ('Mutter', 'M'), 
    ('Baby', 'B');

create type bacteria_taxon as enum (
    'Bacteroides_xylanisolvens',
    'Bifidobacterium_adolescentis',
    'Bifidobacterium_bifidum',
    'Bifidobacterium_breve',
    'Bifidobacterium_longum',
    'Bifidobacterium_spp',
    'Cutibacterium_acnes',
    'Enterococcus_faecalis',
    'Enterococcus_faecium',
    'Escherichia_coli',
    'Klebsiella_oxytoca',
    'Lactococcus_lactis',
    'Staphylococcus_aureus'
);

create temp table taxon_ref (
    taxon_raw varchar,
    taxon bacteria_taxon
);

insert into taxon_ref (taxon_raw, taxon) 
values 
    ('Bacteroides_xylanisolvens', 'Bacteroides_xylanisolvens'), 
    ('Bifidobacterium_adolescentis', 'Bifidobacterium_adolescentis'),
    ('Bifidobacterium_bifidum', 'Bifidobacterium_bifidum'),
    ('Bifidobacterium_breve', 'Bifidobacterium_breve'),
    ('Bifidobacterium_longum', 'Bifidobacterium_longum'),
    ('Bifidobacterium_spp', 'Bifidobacterium_spp'),
    ('Cutibacterium_acnes', 'Cutibacterium_acnes'),
    ('Enterococcus_faecalis', 'Enterococcus_faecalis'),
    ('Enterococcus_faecium', 'Enterococcus_faecium'),
    ('Escherichia_coli', 'Escherichia_coli'),
    ('Escherichia_coli_(rot)', 'Escherichia_coli'),
    ('Escherichia_coli_(weiß)', 'Escherichia_coli'),
    ('Klebsiella_oxytoca', 'Klebsiella_oxytoca'),
    ('Lactococcus_lactis', 'Lactococcus_lactis'),
    ('Staphylococcus_aureus', 'Staphylococcus_aureus');

create temp table date_ref (
    time_cat varchar,
    time_weeks varchar
);

insert into date_ref (time_cat, time_weeks) 
values 
    ('vor', -2), 
    ('2Wochen', 2), 
    ('4Wochen', 4), 
    ('2Monate', 8), 
    ('3Monate', 13), 
    ('6Monate', 26);

create temp table fastqs as
pivot (
    select 
        struct_extract(ID_read, 'ID') as ID
        , "file"
        , 'fastq_' || struct_extract(ID_read, 'read') as "read"
    from (
        select
            "file" 
            , regexp_extract(
                "file",
                'mpimg_L\d+-\d_(B001-\d+)_S\d+_R(1|2)_001.fastq.gz',
                ['ID', 'read']
            ) as ID_read
        from glob(getenv('FASTQS_DIR'))
    )
    where strlen(ID) > 0
) on "read" using first("file") group by ID;

create temp table samples as 
with combined_tmp as (
    select * exclude(taxon_raw)
        , regexp_replace(trim(taxon_raw), ' ', '_', 'g') as taxon_raw
    from (
        select "gDNA from 96-Well" as ID
            , Field2 as taxon_raw
            , regexp_extract(Field3, '^(\D+)-', 1) as relationship_raw
            , regexp_extract(Field3, '-(\w+)$', 1) as time_cat
        from st_read(
            getenv('SAMPLESHEET'),
            layer = 'LibraryPlate1-3',
            open_options = ['HEADERS=FORCE']
        )
        union by name
        select
            Field1 as ID
            , Field2 as gDNA
            , case when Field4 is null then Field3
                else concat(Field3, ' (', Field4, ')')
                end as taxon_raw
            , Field5
            , Field6 as relationship_raw
            , Field7 as time_cat
        from st_read(
            getenv('SAMPLESHEET'),
            layer = 'LibraryPlate4-8'
        )
    )
    where ID is not null
    order by ID
)
select
    row_number() over () as "sample"
    , regexp_replace(samples.ID, '_', '-') as ID
    , regexp_extract(samples.ID, '([BP]\d+|Ctr\d+|Control\d+)_\d+', 1) as donor
    , try_cast(
        coalesce(
            t_ref.taxon, samples.taxon_raw
        ) as bacteria_taxon
    ) as taxon
    , try_cast(
        coalesce(
            r_ref.relationship, samples.relationship_raw
        ) as family_relationship
    ) as relationship
    , try_cast(
        coalesce(
            d_ref.time_weeks, samples.time_cat
        ) as smallint
    ) as time_weeks
from combined_tmp as samples
left join relationship_ref as r_ref
on r_ref.relationship_raw = samples.relationship_raw
left join taxon_ref as t_ref
on t_ref.taxon_raw = samples.taxon_raw
left join date_ref as d_ref
on d_ref.time_cat = samples.time_cat;

create table samplesheet as
select samples.*, fastqs.fastq_1, fastqs.fastq_2 
from fastqs, samples 
where fastqs.ID = samples.ID 
order by samples.ID;