copy (
    with
        vcfs as (
            select * from glob('{{ glob }}')
            where "file" not ilike '%.clean%.parquet'
              and "file" not ilike '%.filter%.parquet'
        ),

        parsed_filename as (
            select "file"
                , regexp_extract(
                    "file",
                    '{{ pat }}',
                    string_split('{{ fields }}', '|')
                ) as extracted
            from vcfs
        ),

        final as (select extracted.*, "file" from parsed_filename)
    select * from final
) to '{{ output }}' (format csv);