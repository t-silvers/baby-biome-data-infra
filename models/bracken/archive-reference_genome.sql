copy (
    with
        sample_info as (
            select * from read_csv('{{ samplesheet }}')
        ),

        bracken_output as (
            select "sample"
                , "name"
                , new_est_reads
                , fraction_total_reads
            from read_parquet('{{ bracken_glob }}', hive_partitioning = false)
        ),

        joined as (
            select s.*, b.* exclude("sample")
            from bracken_output b
            left join sample_info s
            on  b.sample = s.sample
        ),

        top_species_id as (
            select "sample", max(fraction_total_reads) as max_frac
            from joined
            where new_est_reads > pow(10, cast('{{ read_pow }}' as int))
              and fraction_total_reads > cast('{{ read_frac }}' as float)
            group by "sample"
        ),

        final as (
            select * exclude("name")
                , regexp_replace(
                    trim("name"), ' ', '_', 'g'
                ) as reference_genome
            from joined
            where ("sample", fraction_total_reads) in (
                select ("sample", max_frac) from top_species_id
                )
              and split_part("name", ' ', 1) = split_part(species_stdized, '_', 1)
            order by
                cast("sample" as int)
        )
    select * from final
) to '{{ output }}' (format csv);