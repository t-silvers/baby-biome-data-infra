attach '{{ fastqs_db }}' as fastqs_db (read_only);

attach '{{ ref_genomes_db }}' as ref_genomes_db (read_only);

copy (
    select 
        -- TODO: Consider other "patient" groupings.
        --       Must be unique ("family" would fail).
        "sample" as patient
        , "sample"
        , 'lane1' as lane
        , fastq_1
        , fastq_2
    from fastqs_db.sequencing_records
    where "sample" in (
        select "sample" from ref_genomes_db.reference_genomes
        where reference_genome = '{{ reference_genome }}'
    )
) to '{{ output }}' (format csv);