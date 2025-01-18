create type tools as enum (
    select tool from read_csv('{{ maptools }}')
);

create table mapping_progress (
    sample uinteger,
    tool tools,
    primary key ("sample", tool)
);