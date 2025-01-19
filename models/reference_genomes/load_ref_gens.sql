create table if not exists reference_genomes (
    sample uinteger,
    reference_genome varchar,
    new_est_reads uinteger,
    fraction_total_reads float,
    primary key (sample)
);


insert or ignore into reference_genomes
    by name
    select * from new_reference_genomes;