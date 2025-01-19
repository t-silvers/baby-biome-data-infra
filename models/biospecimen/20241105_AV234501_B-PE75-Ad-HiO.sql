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
                'Escherichia_coli' as taxon_plate
                , Family as donor_family
                , "Subject" as relationship
                , Timepoint as timepoint
                , Family || '_' || "Subject" as donor_id
                , ID as isolate_id
            from raw_sample_info
        ),

        final as (
            select * from cleaned
            where isolate_id is not null
        )

    select * from final
) to '{{ output }}' (format csv);