create type if not exists relationship as enum (
    select relationship from read_csv('{{ relationship }}')
);

create type if not exists taxon_plate as enum (
    select taxon from read_csv('{{ taxon_plate }}')
);

create type if not exists timepoint as enum (
    select timepoint from read_csv('{{ timepoint }}')
);

create table if not exists annot_sequencing_records (
    sample uinteger,
    sample_name varchar,
    taxon_plate taxon_plate,
    donor_family varchar,
    relationship relationship,
    donor_id varchar,
    timepoint timepoint,
    isolate_id varchar,
    seqrun varchar,
    fastq_1 varchar,
    fastq_2 varchar,
    primary key (sample)
);

insert or ignore into annot_sequencing_records
    by name
    select * from read_csv('{{ annot_seq }}', hive_partitioning = false);