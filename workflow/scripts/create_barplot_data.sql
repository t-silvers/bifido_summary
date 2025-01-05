-- duckdb -readonly /ptmp/thosi/bifido_summary/data/variants/Bifidobacterium_longum.duckdb

set memory_limit = getenv('MEMORY_LIMIT');
set threads = getenv('SLURM_CPUS_PER_TASK');
set enable_progress_bar = false;

create temp table barplot_data as
with selected_coords as (

    select
        distinct on(chromosome, position)
        chromosome, position

    from
        read_csv(
            '/ptmp/thosi/bifido_summary/results/Bifidobacterium_longum/annot_filtered_calls.csv'
        )
),
coords_calls as (

    select
        "sample"
        , chromosome
        , position
        , reference
        , alternate
        , allelic_read_depth_forward
        , allelic_read_depth_reverse

    from
        annotated_vcfs
    
    where
        (chromosome, position) in (

            select
                (chromosome, position)
            
            from
                selected_coords

        )

)
select * from coords_calls;


create temp table final_barplot_data as
with exploded as (
    select
        "sample"
        , chromosome
        , "position"
        , unnest([reference, alternate[1]]) as allele
        , unnest(allelic_read_depth_forward) as forward
        , unnest(allelic_read_depth_reverse) as reverse
        , case when unnest(['reference', 'alternate']) = 'alternate' then true else false end as is_alternate

    from
        barplot_data
),
unpivoted as (
	unpivot (
		select * from exploded
		where allele is not null
	) 
	on forward, reverse
	into 
		name read_orientation
		value depth	
),
final as (

    select

        * exclude(read_orientation, depth)
        , read_orientation
        , depth

    from
        unpivoted
    
    where
        depth > 0

)

select * from final;

copy (select * from final_barplot_data) to '/u/thosi/dev/projects/bifido_summary/dev/20241114_barplot_data.csv'