set memory_limit = getenv('MEMORY_LIMIT');
set threads = getenv('SLURM_CPUS_PER_TASK');
set enable_progress_bar = false;

-- Read filtered calls
create temp table geno as
with filtered_calls as (
    select * from read_csv(getenv('GENO'))
)
select ID, chromosome, "position", allele
from filtered_calls
-- Remove samples with too few calls?
where ID in (
    select ID from filtered_calls 
    group by ID 
    having count(*) >= (
        select count(distinct(chromosome, "position")) * .05 
        from filtered_calls
    )
);

create temp table geno2 as
select * from geno where 
-- Remove positions with too few calls?
position in (
    select position from geno 
    group by position 
    having count(*) >= (
        select count(distinct(ID)) * .5
        from geno
    )
);

-- Impute missing coordinates as 'N'
create temp table imputed as
select
    ut.chromosome,
    ut.position,
    ua.ID,
    ifnull(t.allele, 'N') as allele
from
    (select distinct chromosome, "position" from geno2) ut
cross join
    (select distinct ID from geno2) ua
left join geno2 t on
    t.chromosome = ut.chromosome and
    t.position = ut.position and
    t.ID = ua.ID
order by ua.ID, ut.chromosome, ut.position;

-- Output as fasta
copy (
    select '>' || ID || chr(10) || string_agg(allele, '')
    from imputed
    group by ID
    order by ID
) to '/dev/stdout' (delimiter '', header false, quote '');