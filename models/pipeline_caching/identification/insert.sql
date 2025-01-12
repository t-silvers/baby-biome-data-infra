create temp table identification_snapshot as
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

-- NOTE: List Update is not supported
insert or replace into identification_progress
    by name
    select unnest(tool) as tool
    from identification_snapshot;