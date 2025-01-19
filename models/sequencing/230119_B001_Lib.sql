copy (
    with
        raw_seq_info as (
            select * from read_csv(
                '{{ input }}',
                auto_detect = false,
                columns = {
                    'Original sample plate (or first arrayed plate, if source is tubes)': varchar,
                    'Well': varchar,
                    'i5 name': varchar,
                    'i7 name': varchar,
                    'SampleName (optional)': varchar,
                    'Library prep method (optional)': varchar,
                    'Notes (optional)': varchar
                },
                skip = 14
            )
        ),

        cleaned as (
            select
                "SampleName (optional)" as sample_name
                , case
                    when sample_name ilike 'control%' then 'control'
                    when starts_with(sample_name, 'B') then regexp_replace(sample_name, '-', '_')
                    else sample_name
                end as isolate_id
                , cast(
                    regexp_extract(
                        "Original sample plate (or first arrayed plate, if source is tubes)",
                        '^Library Platte(\d+)$',
                        1
                    )
                    as usmallint
                ) as plate
                , "Well" as well
                , "i5 name" as barcode_1
                , "i7 name" as barcode_2
                , "Notes (optional)" as notes
            from raw_seq_info
        ),

        final as (
            select * from cleaned
            where sample_name is not null
        )

    select * from final
) to '{{ output }}' (format csv);