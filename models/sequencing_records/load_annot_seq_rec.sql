create table if not exists annot_sequencing_records (
    sample uinteger,
    sample_name varchar,
    taxon_plate varchar,
    donor_family varchar,
    relationship varchar,
    donor_id varchar,
    timepoint varchar,
    isolate_id varchar,
    seqrun varchar,
    fastq_1 varchar,
    fastq_2 varchar,
    primary key (sample)
);

insert or ignore into annot_sequencing_records
    by name
    select * from read_csv('{{ annot_seq }}', hive_partitioning = false);