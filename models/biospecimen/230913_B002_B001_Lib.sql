-- keylab/projects/2020_infantMbiome/Familien/Baby002/Library_prep/B002_Isolates_with_allfaecalisCHECKmeike_MF.xlsx

load spatial;

copy (
    with
        raw_sample_info as (
            select * from st_read(
                '{{ input }}',
                open_options = ['HEADERS=FORCE']
            )
        ),

        cleaned as (
            select
                species as taxon_plate
                , regexp_extract(id, '([BP]\d+|Ctr\d+|Control\d+)_\d+', 1) as donor_family
                , "subject" as relationship
                , timepoint
                , donor_family || '_' || "subject" as donor_id
                , id as isolate_id
            from raw_sample_info
        ),

        final as (
            select * from cleaned
            where isolate_id is not null
        )

    select * from final
) to '{{ output }}' (format csv);