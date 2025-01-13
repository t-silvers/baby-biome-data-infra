create type tools as enum (
    select tool from read_csv('{{ idtools }}')
);

create table identification_progress (
    sample uinteger,
    tool tools,
    primary key ("sample", tool)
);