copy (
    with
        trimmed_fastqs as (
            select * from glob('{{ glob }}')
        ),

        parsed_filename as (
            select "file"
                , extracted.* exclude ("read")
                , 'trim_fastq_' || extracted.read as "read"
            from (
                select "file"
                    , regexp_extract(
                        "file",
                        '{{ pat }}',
                        ['sample', 'read']
                    ) as extracted
                from trimmed_fastqs
            )
        ),

        final as (
            pivot parsed_filename
            on "read" 
            using first("file") 
            group by "sample"
        )

    select * from final
) to '{{ output }}' (format csv);