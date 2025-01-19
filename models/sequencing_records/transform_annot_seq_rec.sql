attach '{{ fastqs_db }}' as fastqs_db (read_only);

create temp table sequencing_info as
select * from read_csv('{{ sequencing_glob }}', hive_partitioning = true, union_by_name = true);

create temp table labeled_fastqs as
with
    -- NOTE: Required due to name collisions in paths
    -- TODO: Move to sequencing transform models
    sequencing_info_for_match as (
        select *
            , case
                when seqrun = '20241105_AV234501_B-PE75-Ad-HiO' then concat('%', sample_name, '_R', '%')
                when seqrun = '230119_B001_Lib' then concat('%', sample_name, '_S', '%')
                when seqrun = '230913_B002_B001_Lib' then concat('%', sample_name, '_S', '%')
                when seqrun = '240704_B002_B001_Lib_AVITI_reseq' then concat('%', sample_name, '_R', '%')
                else null
            end as name_to_match
        from sequencing_info
    ),

    matched_fastqs as (
        select t1.*, t2.* exclude(seqrun, name_to_match)
        from fastqs_db.sequencing_records t1
            , sequencing_info_for_match t2
        where t1.seqrun = t2.seqrun
          and t1.fastq_1 like t2.name_to_match
          and t1.fastq_2 like t2.name_to_match
    ),

    final as (
        select distinct "sample", sample_name, isolate_id, seqrun, fastq_1, fastq_2
        from matched_fastqs
    )

select * from final;

create temp table biospecimen_info as
select * from read_csv('{{ biospecimens_glob }}', hive_partitioning = true, union_by_name = true);

-- TODO: Move to biospecimen transform models
create temp table cleaned_biospecimen_info as
with
    cleaned_relationship as (
        select seqrun, isolate_id
            , case
                -- baby (focal)
                when relationship ilike 'baby%' then 'baby'
                -- sibling
                when relationship ilike 'kind%' then 'sibling'
                when relationship ilike 'sibling%' then 'sibling'
                -- mother
                when relationship ilike 'mutter' then 'mother'
                when relationship ilike 'mother' then 'mother'
                -- father
                when relationship ilike 'vater' then 'father'
                when relationship ilike 'father' then 'father'
                else null
            end as relationship
        from biospecimen_info
        where relationship is not null
    ),

    cleaned_timepoint_unit as (
        select seqrun, isolate_id
            , timepoint
            , case
                -- before
                when timepoint_unit ilike 'vor' then 'before'
                when timepoint_unit ilike 'before' then 'before'
                -- months
                when timepoint_unit ilike 'm' then 'months'
                when timepoint_unit ilike 'monate' then 'months'
                when timepoint_unit ilike 'months' then 'months'
                -- weeks
                when timepoint_unit ilike 'w' then 'weeks'
                when timepoint_unit ilike 'wochen' then 'weeks'
                when timepoint_unit ilike 'weeks' then 'weeks'
                else null
            end
            as collection_interval_unit
        from (
                select seqrun, isolate_id
                    , timepoint
                    , regexp_extract(timepoint, '(\D+)$', 1) as timepoint_unit
                from biospecimen_info
            )
        where collection_interval_unit is not null
    ),

    cleaned_timepoint as (
        select seqrun, isolate_id
            , timepoint
            , concat_ws(
                ' ', cast(timepoint_value as varchar), collection_interval_unit
            ) as collection_interval_category
            , cast(
                round(
                    case
                        -- before
                        when collection_interval_unit = 'before' then -1
                        -- months, 1 day ~= (365 * 4 + 1) / (12 * 4)
                        when collection_interval_unit = 'months' then timepoint_value * ((365 * 4 + 1) / (12 * 4))
                        -- weeks
                        when collection_interval_unit = 'weeks' then timepoint_value * 7
                        else null
                    end
                ) || ' days'
                as interval
            ) as collection_interval
        from (
            select seqrun, isolate_id
                , timepoint
                , try_cast(
                    regexp_extract(timepoint, '^(\d+)', 1)
                    as usmallint
                ) as timepoint_value
                , collection_interval_unit
            from cleaned_timepoint_unit
        )
    ),

    cleaned_taxa as (
        select seqrun, isolate_id
            , case
                -- Bacteroides ovatus/xylanisolvens
                when taxon_plate ilike 'bacteroides%' then 'Bacteroides_ovatus_xylanisolvens'
                -- Bifidobacterium spp
                when (
                    taxon_plate ilike 'bifidobacterium%'
                    or taxon_plate ilike 'bifidobakterien%'
                ) then 'Bifidobacterium_spp'
                -- Enterococcus faecalis
                when taxon_plate ilike '%enterococcus%' then 'Enterococcus_faecalis'
                -- Escherichia coli
                when (
                    taxon_plate ilike 'escherichia%'
                    and taxon_plate not ilike 'escherichia%wei%'
                    and taxon_plate not ilike 'escherichia%rot%'
                ) then 'Escherichia_coli'
                -- Escherichia coli (red)
                when taxon_plate ilike 'escherichia%rot%' then 'Escherichia_coli_red'
                -- Escherichia coli (white)
                when taxon_plate ilike 'escherichia%wei%' then 'Escherichia_coli_white'
                -- Klebsiella oxytoca
                when taxon_plate ilike 'klebsiella%' then 'Klebsiella_oxytoca'
                -- Lacticaseibacillus casei/paracasei/rhamnosus
                when taxon_plate ilike 'lacticaseibacillus%' then 'Lacticaseibacillus_casei_paracasei_rhamnosus'
                -- Staphylococcus aureus
                when taxon_plate ilike 'staphylococcus%' then 'Staphylococcus_aureus'
                else null
            end as taxon_plate
        from biospecimen_info
    ),

    joined as (
        select
            t4.taxon_plate
            , t1.donor_family
            , t2.relationship
            , t1.donor_id
            , t3.collection_interval_category as timepoint
            , t1.isolate_id
            , t1.seqrun
        from biospecimen_info t1
        join cleaned_relationship t2 on (
            t1.seqrun = t2.seqrun and t1.isolate_id = t2.isolate_id
        )
        join cleaned_timepoint t3 on (
            t1.seqrun = t3.seqrun and t1.isolate_id = t3.isolate_id
        )
        join cleaned_taxa t4 on (
            t1.seqrun = t4.seqrun and t1.isolate_id = t4.isolate_id
        )
    ),

    final as (select distinct * from joined)

select * from final;

copy (
    select t1.*, t2.* exclude(seqrun, isolate_id)
    from labeled_fastqs t1
    left join cleaned_biospecimen_info t2
        on t1.seqrun = t2.seqrun
       and t1.isolate_id = t2.isolate_id
) to '{{ annot_seq }}' (format csv);