attach '{{ fastqs_db }}' as fastqs_db (read_only);

attach '{{ ref_genomes_db }}' as ref_genomes_db (read_only);

copy (
    select
        t1.sample
        , t1.fastq_1
        , t1.fastq_2
        , t2.reference_genome
    from fastqs_db.sequencing_records t1
    join (
        select * from ref_genomes_db.reference_genomes
        where reference_genome is not null
    ) t2
        on t1.sample = t2.sample
) to '{{ output }}' (format csv);