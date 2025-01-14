copy (
    select * from glob('{{ glob }}')
    where "file" not ilike '%.clean%.parquet'
      and "file" not ilike '%.filter%.parquet'
) to '{{ output }}' (format csv);