copy (
    with
        taxprofiler_samplesheet as (
            select 
                "sample"
                , seqrun || '_' || "sample" as run_accession
                , '{{ instrument_platform }}' as instrument_platform
                , fastq_1
                , fastq_2
            from sequencing_records
        )

    select * from taxprofiler_samplesheet
) to '{{ samplesheet }}' (format csv);