create temp table mapping_snapshot as
with
    raw_progress as (
        select info.* from (
            select
                regexp_extract(
                    "file",
                    '{{ pat }}',
                    ['tool', 'sample']
                ) as info
            from glob('{{ glob }}')
        )
        where info.sample != ''
    ),

    final as (
        select "sample"
            , array_sort(array_agg(distinct tool)) as tool
        from raw_progress
        group by "sample"
    )
select * from final;

insert or replace into mapping_progress
    by name
    select * from mapping_snapshot;