create temp table new_reference_genomes as
with
    bracken as (
        select "sample"
            , "name"
            , new_est_reads
            , fraction_total_reads
        from read_parquet('{{ bracken_glob }}', hive_partitioning = false)
    ),

    top_species_id as (
        select "sample", max(fraction_total_reads) as max_frac
        from bracken
        group by "sample"
    ),

    final as (
        select * exclude("name")
            , regexp_replace(
                trim("name"), ' ', '_', 'g'
            ) as reference_genome
        from bracken
        where ("sample", fraction_total_reads) in (
            select ("sample", max_frac) from top_species_id
        )
    )
select * from final;

insert or ignore into reference_genomes
    by name
    select * from new_reference_genomes;

copy (
    with
        samplesheet as (
            select * from read_csv('{{ samplesheet }}')
        ),

        joined as (
            select t1.*, t2.* exclude("sample")
            from samplesheet t1
            left join reference_genomes t2
            where t1.sample = t2.sample
                and t2.reference_genome is not null
        ),

        -- Filter based on support
        bracken_read_filtered as (
            select "sample" from joined
            where new_est_reads > pow(10, cast('{{ read_pow }}' as int))
              and fraction_total_reads > cast('{{ read_frac }}' as float)
        ),

        -- Check that plate-based genus agrees w seq-based
        agreement_filtered as (
            select "sample" from joined
            where split_part(species_stdized, '_', 1) = split_part(reference_genome, '_', 1)
        ),

        -- Reference must be available
        available_reference_filtered as (
            select "sample" from joined
            where reference_genome in string_split('{{ available_genomes }}', '|')
        ),

        final as (
            select * from joined
            where "sample" in (select "sample" from bracken_read_filtered)
              and "sample" in (select "sample" from agreement_filtered)
              and "sample" in (select "sample" from available_reference_filtered)
        )
    select * from final
) to '{{ output }}' (format csv);