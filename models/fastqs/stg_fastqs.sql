copy (
    select * from glob('{{ glob }}')
) to '{{ staged }}' (format csv);