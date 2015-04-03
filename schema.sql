drop table if exists monthlyData;
create table monthlyData (
  name text not null,
  value int not null,
  date text not null,
  number int not null,
  id text PRIMARY KEY
);
