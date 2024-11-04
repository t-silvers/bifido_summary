

-- Filling in missing values
create temp table imputed as
select
    ut.chromosome,
    ut.position,
    ut.reference,
    ua.sample,
    coalesce(t.allele, 'N') as allele
from
    (select distinct chromosome, "position", reference from filtered_calls) ut
cross join
    (select distinct sample from filtered_calls) ua
left join filtered_calls t on
    t.chromosome = ut.chromosome and
    t.position = ut.position and
    t.reference = ut.reference and
    t.sample = ua.sample;