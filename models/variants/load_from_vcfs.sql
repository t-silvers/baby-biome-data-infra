set preserve_insertion_order = false;
set max_temp_directory_size = '{{ max_temp_directory_size }}';

create table if not exists variants (
    -- ~*~ Required ~*~
    contig varchar,
    start_pos uinteger,
    sample uinteger,
    alleles varchar[],
    -- ~*~ Extra ~*~
    -- For precise variants, END is POS + length of REF allele - 1, and the for imprecise variants the corresponding best estimate.
    end_pos uinteger,
    --
    qual decimal(6, 1),
    -- ~*~ Extra: info ~*~
    --
    info_AD usmallint[],
    --
    info_ADF usmallint[],
    --
    info_ADR usmallint[],
    -- Depth of coverage
    info_DP usmallint,
    -- end-placement probability score
    info_EPP decimal(4, 1)[],
    -- Mapping quality
    info_MQ decimal(4, 1),
    -- mean mapping quality of observed reference, alternate alleles
    info_MQM decimal(4, 1)[],
    -- strand-bias probability score
    info_SP decimal(4, 1)[],
    -- variant type
    info_TYPE varchar[],
    -- ~*~ Extra: format ~*~
    --
    format_GT varchar,
    -- phred-scaled genotype likelihoods rounded to the closest integer (and otherwise defined precisely as the GL field) (Integers)
    format_PL float[],
    --
    format_SP decimal(4, 1),
    primary key (contig, start_pos, "sample", end_pos)
);

create temp table new_variants as
select * from read_parquet(
    '{{ vcf_pq_glob }}',
    union_by_name = true,
    hive_partitioning = false
);

-- NOTE: Relies on automatic type conversion for "contig" field
insert or ignore into variants
    by name
    select * from new_variants;

copy (
    with
        snvs as (
            select "sample", contig, start_pos, alleles
            from variants 
            where not list_contains(info_TYPE, 'indel')
        ),
        
        shared_positions as (
            select contig, start_pos
            from snvs
            group by contig, start_pos
            having count(*) >= cast('{{ frac_cohort_core }}' as float) * (
                select count(distinct "sample")
                from variants
            )
        ),

        shared_snvs as (
            select * from snvs
            where (contig, start_pos) in (
                select (contig, start_pos)
                from shared_positions
            )
        ),

        final as (
            select "sample"
                , contig
                , start_pos as position
                , alleles[1] as allele
            from shared_snvs
        )
    select * from final
) to '{{ output }}' (format parquet);