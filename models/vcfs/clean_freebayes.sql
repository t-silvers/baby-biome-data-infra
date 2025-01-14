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
                , columns('format_.*_AD') as info_AD
                , info_DP
                , info_EPP
                , info_EPPR
                , info_MQM
                , info_MQMR
                , info_SAP
                , info_SRP
                , info_TYPE
                , columns('format_.*_GT') as format_GT
                , columns('format_.*_GL') as format_GL
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
                , cast(quality as decimal(6, 1)) as qual
                , case
                    when list_any_value("filter") is null then ['PASS']
                    else "filter"
                  end as "filter"
                , array_concat([reference], alternate) as alleles

                -- info fields
                , array_transform(info_AD, x -> cast(x as usmallint)) as info_AD
                , [] as info_ADF
                , [] as info_ADR
                , cast(info_DP as usmallint) as info_DP
                , array_transform(
                    array_concat(
                        [info_EPPR], info_EPP),
                        x -> cast(x as decimal(4, 1)
                    )
                ) as info_EPP
                , null as info_MQ
                , array_transform(
                    array_concat(
                        [info_MQMR], info_MQM),
                        x -> cast(x as decimal(4, 1)
                    )
                ) as info_MQM
                , array_transform(
                    array_concat(
                        [info_SRP], info_SAP),
                        x -> cast(x as decimal(4, 1)
                    )
                ) as info_SP
                , array_transform(
                    info_TYPE,
                    x -> case
                        when x = 'complex' then 'indel'
                        when x = 'del' then 'indel'
                        when x = 'ins' then 'indel'
                        when x = 'mnp' then 'indel'
                        when x = 'snp' then x
                        else null
                    end
                ) as info_TYPE

                -- format fields
                , cast(format_GT as varchar) as format_GT
                , array_transform(format_GL, x -> cast(x as float)) as format_PL
                , [] as format_SP

            from raw_vcf
        ),

        final as (select * from cleaned)
    select * from final
) to '{{ cleaned }}' (format parquet);