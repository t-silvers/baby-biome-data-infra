copy (
    with
        samplesheet as (
            select * from read_csv('{{ input }}')
            -- TODO: Should these be retained?
            where id like 'B%'
        ),

        completed_samples as (
            select "sample" from mapping_progress
            where tool = 'bactmap'
        ),

        bactmap_samplesheet as (
            select 
                "sample"
                , fastq_1
                , fastq_2
            from samplesheet
            where reference_genome = '{{ reference_genome }}'
        ),

        final as (
            select * from bactmap_samplesheet
            where "sample" not in (
                select "sample" from completed_samples
            )
        )
    select * from final
) to '{{ output }}' (format csv);