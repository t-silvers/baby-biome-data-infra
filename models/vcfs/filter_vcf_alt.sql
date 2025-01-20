set max_temp_directory_size = '300GB';

copy (
    with
        filtered_vcf as (
            select * from read_parquet(
                '{{ filtered }}', hive_partitioning = false
            )
        ),

        alt_only as (
            select *
            from filtered_vcf
            where
                -- Filters to only ALT calls passing a MAF of .95
                (info_AD[1] / array_reduce(info_AD, (x, y) -> x + y)) < (1 - cast('{{ maf }}' as float))
        ),

        final as (select * from alt_only)

    select * from final
) to '{{ filtered_alt }}' (format parquet);