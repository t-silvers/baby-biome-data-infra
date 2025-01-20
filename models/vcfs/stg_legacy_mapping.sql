copy (
    select * from read_parquet(
        '{{ input }}',
        filename = true,
        hive_partitioning = false
    )
) to '{{ stg_vcf }}' (format parquet);