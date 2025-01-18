create table if not exists sequencing_records (
    sample uinteger,
    seqrun varchar,
    fastq_1 varchar,
    fastq_2 varchar,
    primary key (sample)
);

create sequence if not exists "sample";

create temp table new_sequencing_records as
select * from read_csv('{{ glob }}', hive_partitioning = true)
order by seqrun, fastq_1;

-- Add new FASTQs to the sequencing records table, ignoring existing records
insert or ignore into sequencing_records
    by name
    select nextval('sample') as "sample", *
    from new_sequencing_records;