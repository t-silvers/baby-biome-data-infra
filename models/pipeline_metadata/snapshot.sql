copy (
    with
        artifacts as (
            select * from glob('{{ glob }}')
        ),

        parsed as (
            select "file"
                , regexp_extract(
                    "file",
                    '{{ pat }}',
                    string_split('{{ fields }}', '|')
                ) as fields
            from artifacts
        ),

        final as (
            select fields.*, "file" from parsed
        )
    select * from final
) to '{{ output }}' (format csv);