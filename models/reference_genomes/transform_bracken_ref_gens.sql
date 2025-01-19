set preserve_insertion_order = false;
set max_temp_directory_size = '{{ max_temp_directory_size }}';

attach '{{ annot_seq_db }}' as seq_records (read_only);

create temp table new_reference_genomes as
with
    plate_taxa as (
        select "sample", taxon_plate 
        from seq_records.annot_sequencing_records
    ),

    raw_bracken as (
        select "sample"
            , "name"
            , new_est_reads
            , fraction_total_reads
        from read_parquet('{{ bracken_glob }}', hive_partitioning = false)
    ),

    cleaned_bracken as (
        select * exclude("name")
            , regexp_replace(
                trim("name"), ' ', '_', 'g'
            ) as taxon_seq
        from raw_bracken
    ),

    top_species_by_reads_frac as (
        select * exclude(ranked_taxa)
        from (
            select *
                , row_number() over (
                    partition by "sample" order by fraction_total_reads desc
                ) ranked_taxa
            from cleaned_bracken
        )
        where ranked_taxa = 1
    ),

    joined as (
        select t1.*, t2.* exclude("sample")
        from plate_taxa t1
        left join top_species_by_reads_frac t2
            on t1.sample = t2.sample
    ),

    -- Filter based on sequencing support
    bracken_read_filtered as (
        select "sample" from joined
        where new_est_reads > pow(10, cast('{{ read_pow }}' as int))
          and fraction_total_reads > cast('{{ read_frac }}' as float)
    ),
    
    -- Check that plate-based genus agrees w seq-based
    agreement_filtered as (
        select "sample" from joined
        where (
            split_part(taxon_plate, '_', 1) = split_part(taxon_seq, '_', 1)
            or taxon_plate is null
        )
    ),

    -- Reference must be available
    available_reference_filtered as (
        select "sample" from joined
        where list_contains(
            string_split('{{ available_genomes }}', '|'), taxon_seq
        )
    ),

    reference_genomes as (
        select "sample", taxon_seq, taxon_seq as reference_genome
        from joined
        where "sample" in (select "sample" from bracken_read_filtered)
          and "sample" in (select "sample" from agreement_filtered)
          and "sample" in (select "sample" from available_reference_filtered)
    ),

    final as (
        select t1.*, t2.reference_genome
        from joined t1
        left join reference_genomes t2
            on t1.sample = t2.sample
           and t1.taxon_seq = t2.taxon_seq
    )

select * from final;

copy (select * from new_reference_genomes) to '{{ ref_genomes }}' (format csv);