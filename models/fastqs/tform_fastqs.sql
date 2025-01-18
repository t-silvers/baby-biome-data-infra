copy (
    with
        fastqs_glob as (
            select * from read_csv('{{ staged }}')
        ),

        parsed_filename as (
            select "file"
                , extracted.* exclude ("read")
                , 'fastq_' || extracted.read as "read"
            from (
                select "file"
                    , regexp_extract(
                        "file",
                        '{{ pat }}',
                        ['seqrun', 'id', 'family', 'read']
                    ) as extracted
                from fastqs_glob
            )
        ),

        pivot_on_reads as (
            pivot parsed_filename
            on "read" 
            using first("file") 
            group by seqrun, family, id
        ),

        final as (
            select * exclude(id)
                , replace(id, '-', '_') as id
            from pivot_on_reads
        )

    select columns('fastq_[1,2]') from final
) to '{{ transformed }}' (format csv);