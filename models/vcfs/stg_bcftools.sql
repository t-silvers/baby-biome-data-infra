copy (
    with
        raw_vcf as (
            select
                try_cast(
                    regexp_extract("filename", '/(\d+).parquet$', 1)
                    as uinteger
                ) as "sample"
                , chromosome
                , position
                , reference
                , alternate
                , quality
                , "filter"
                , info_AD
                , info_ADF
                , info_ADR
                , info_MQ
                , info_DP
                , info_INDEL
                , columns('format_.*_GT') as format_GT
                , columns('format_.*_PL') as format_PL
                , columns('format_.*_SP') as format_SP
            from read_parquet(
                '{{ input }}',
                filename = true,
                hive_partitioning = false
            )
        ),

        cleaned as (
            select chromosome as contig
                , position as start_pos
                , "sample"
                , position + length(reference) - 1 as end_pos
                , cast(quality as decimal(4, 1)) as qual
                , case
                    when list_any_value("filter") is null then ['PASS']
                    else "filter"
                  end as "filter"
                , array_concat([reference], alternate) as alleles

                -- info fields
                , array_transform(info_AD, x -> cast(x as usmallint)) as info_AD
                , array_transform(info_ADF, x -> cast(x as usmallint)) as info_ADF
                , array_transform(info_ADR, x -> cast(x as usmallint)) as info_ADR
                , cast(info_DP as usmallint) as info_DP
                , [] as info_EPP
                , cast(info_MQ as decimal(4, 1)) as info_MQ
                , [] as info_MQM
                , [] as info_SP
                , case
                    when info_INDEL is true then ['indel']
                    when info_INDEL is false then ['snp']
                    else [null]
                end
                as info_TYPE
                
                -- format fields
                , cast(format_GT as varchar) as format_GT
                , array_transform(format_PL, x -> cast(x as usmallint)) as format_PL
                , cast(format_SP as float) as format_SP

            from raw_vcf
        ),

        final as (select * from cleaned)
    select * from final
) to '{{ stg_vcf }}' (format parquet);