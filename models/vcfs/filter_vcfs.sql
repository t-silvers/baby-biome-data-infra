copy (
    with
        cleaned_vcf as (
            select * from read_parquet(
                '{{ cleaned }}',
                filename = true,
                hive_partitioning = false
            )
        ),

        filtered as (
            select * exclude("filter")
            from cleaned_vcf
            where
                'PASS' = any("filter")
                and qual >= cast('{{ quality }}' as float)
                -- Filters to only ALT calls passing a MAF of .95
                and (info_AD[1] / array_reduce(info_AD, (x, y) -> x + y)) < (1 - cast('{{ maf }}' as float))
                and array_reduce(info_AD, (x, y) -> x + y) >= cast('{{ ad }}' as float)
                and (
                    list_any_value(info_ADF) is null
                    or array_reduce(info_ADF, (x, y) -> x + y) >= cast('{{ ad_strand }}' as float)
                )
                and (
                    list_any_value(info_ADR) is null
                    or array_reduce(info_ADR, (x, y) -> x + y) >= cast('{{ ad_strand }}' as float)
                )
                and info_DP >= cast('{{ dp }}' as float)
                and (
                    list_any_value(info_EPP) is null
                    or (
                        info_EPP[1] < cast('{{ epp }}' as float)
                        and list_min(info_EPP[2:]) < cast('{{ epp }}' as float)
                    )
                )
                and (
                    info_MQ is null
                    or info_MQ >= cast('{{ mq }}' as float)
                )
                and (
                    list_any_value(info_MQM) is null
                    or (
                        info_MQM[1] >= cast('{{ mq }}' as float)
                        and list_max(info_MQM[2:]) >= cast('{{ mq }}' as float)
                        and (
                            abs(list_max(info_MQM) - info_MQM[1]) < 10
                            and (list_max(info_MQM) / (info_MQM[1] + .1)) >= .4
                            and (list_max(info_MQM) / info_MQM[1]) <= 2.5
                        )
                    )
                )
                and (
                    list_any_value(info_SP) is null
                    or (
                        info_SP[1] < cast('{{ sp }}' as float)
                        and list_min(info_SP[2:]) < cast('{{ sp }}' as float)
                    )
                )
                and (
                    format_SP is null
                    or format_SP < cast('{{ sp }}' as float)
                )
        ),

        final as (select * from filtered)
    select * from final
) to '{{ filtered }}' (format parquet);