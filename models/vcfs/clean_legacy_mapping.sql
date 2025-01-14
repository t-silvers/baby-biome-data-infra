copy (
    select * from read_parquet(
        '{{ input }}',
        filename = true,
        hive_partitioning = false
    )
) to '{{ cleaned }}' (format parquet);