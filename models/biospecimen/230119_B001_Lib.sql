load spatial;

copy (
    with
        raw_sample_info_sheet1 as (
            select * from st_read(
                '{{ input }}',
                layer = 'LibraryPlate1-3',
                open_options = ['HEADERS=FORCE']
            )
        ),

        raw_sample_info_sheet2 as (
            select * from st_read(
                '{{ input }}',
                layer = 'LibraryPlate4-8'
            )
        ),

        sample_info_sheet1 as (
            select "gDNA from 96-Well" as isolate_id
                , Field2 as taxon_plate
                , regexp_extract(Field3, '^(\D+)[- ]', 1) as relationship
                , regexp_extract(Field3, '[- ](\w+)$', 1) as timepoint
            from raw_sample_info_sheet1
        ),

        sample_info_sheet2 as (
            select Field1 as isolate_id
                , Field2 as gDNA
                , case
                    when Field4 is null then Field3
                    else concat(Field3, ' (', Field4, ')')
                end as taxon_plate
                , Field5
                , Field6 as relationship
                , Field7 as timepoint
            from raw_sample_info_sheet2
        ),

        combined as (
            select * exclude(taxon_plate)
                , regexp_replace(
                    trim(taxon_plate), ' ', '_', 'g'
                ) as taxon_plate
            from (select * from sample_info_sheet1)
            union by name
            select * from sample_info_sheet2
        ),

        cleaned as (
            select
                taxon_plate
                , regexp_extract(isolate_id, '([BP]\d+|Ctr\d+|Control\d+)_\d+', 1) as donor_family
                , relationship
                , timepoint
                , donor_family || '_' || relationship as donor_id
                , isolate_id
            from combined
        ),

        final as (
            select * from cleaned
            where isolate_id is not null
        )

    select * from final
) to '{{ output }}' (format csv);