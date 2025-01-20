set max_temp_directory_size = '300GB';

copy (
    with
        staged_vcf as (
            select * from read_parquet(
                '{{ stg_vcf }}',
                filename = true,
                hive_partitioning = false
            )
        ),

        filtered as (
            select * exclude("filter")
            from staged_vcf
            where
                'PASS' = any("filter")

                -- [the QUAL field] estimates the probability that there is a 
                -- polymorphism at the loci described by the record. In freebayes, 
                -- this value can be understood as
                -- 
                --         1 - P(locus is homozygous given the data).
                -- 
                -- It is recommended that users use this value to filter their 
                -- results, rather than accepting anything output by freebayes 
                -- as ground truth.

                -- In simulation, the receiver-operator characteristic (ROC) 
                -- tends to have a very sharp inflection between Q1 and Q30, 
                -- depending on input data characteristics, and a filter setting 
                -- in this range should provide decent performance. Users are 
                -- encouraged to examine their output and both variants which 
                -- are retained and those they filter out. Most problems tend 
                -- to occur in low-depth areas, and so users may wish to remove 
                -- these as well, which can also be done by filtering on [DP].
                and qual >= cast('{{ quality }}' as float)

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