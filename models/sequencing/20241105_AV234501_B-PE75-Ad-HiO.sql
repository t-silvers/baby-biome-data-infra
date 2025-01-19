copy (
    with
        raw_seq_info as (
            select * from read_csv(
                '{{ input }}',
                auto_detect = false,
                columns = {
                    'Original sample plate (or first arrayed plate, if source is tubes)': varchar,
                    'Well': varchar,
                    'Library plate name': varchar,
                    'Library plate well': varchar,
                    'Barcode 1 (Plate or Row)': varchar,
                    'Barcode 2 (Well or Column)': varchar,
                    'SampleName (optional)': varchar,
                    'Library prep method (optional)': varchar,
                    'Notes (optional)': varchar
                },
                skip = 13
            )
        ),

        cleaned as (
            select
                -- NOTE: SeqCore paths always (?) use '-' for sample names
                regexp_replace("SampleName (optional)", '_', '-') as sample_name
                -- NOTE: Use '_' in isolate_ids
                , case
                    when sample_name ilike 'control%' then 'control'
                    when starts_with(sample_name, 'B') then regexp_replace(sample_name, '-', '_')
                    when starts_with(sample_name, 'P') then regexp_replace(sample_name, '-', '_')
                    when starts_with(sample_name, 'plate') then sample_name
                    else null
                end as isolate_id
                , cast(
                    regexp_extract(
                        "Library plate name",
                        '^Platte_(\d+)$',
                        1
                    )
                    as usmallint
                ) as plate
                , "Library plate well" as well
                , "Barcode 1 (Plate or Row)" as barcode_1
                , "Barcode 2 (Well or Column)" as barcode_2
                , "Notes (optional)" as notes
            from raw_seq_info
        ),

        final as (
            select * from cleaned
            where sample_name is not null
        )

    select * from final
) to '{{ output }}' (format csv);