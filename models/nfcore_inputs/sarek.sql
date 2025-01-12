copy (
    with
        samplesheet as (
            select * from read_csv('{{ input }}')
        ),

        completed_samples as (
            select "sample" from mapping_progress
            where tool not ilike 'sarek%'
        ),

        sarek_samplesheet as (
            select 
                -- TODO: Consider other "patient" groupings.
                --       Must be unique ("family" would fail).
                "sample" as patient
                , "sample"
                , 'lane1' as lane
                , fastq_1
                , fastq_2
            from samplesheet
            where reference_genome = '{{ reference_genome }}'
        ),

        final as (
            select * from sarek_samplesheet
            where "sample" not in (
                select "sample" from completed_samples
            )
        )
    select * from final
) to '{{ output }}' (format csv);