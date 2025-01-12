create type genome as enum (
    select genome from read_csv('{{ genomes }}')
);

create table reference_genomes (
    sample uinteger,
    reference_genome genome,
    new_est_reads uinteger,
    fraction_total_reads float,
    primary key (sample)
);