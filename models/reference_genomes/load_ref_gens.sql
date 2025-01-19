create table if not exists reference_genomes (
    sample uinteger,
    taxon_plate varchar,
    taxon_seq varchar,
    new_est_reads uinteger,
    fraction_total_reads float,
    reference_genome varchar,
    primary key (sample)
);

insert or ignore into reference_genomes
    by name
    select * from read_csv('{{ ref_genomes }}');