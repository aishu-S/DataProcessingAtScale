create table users(
userid int primary key not null,
name text not null
);

create table movies(
movieid int primary key not null,
title text not null
);

create table taginfo(
tagid int primary key,
content text
);

create table genres(
genreid int primary key,
name text
);

create table ratings(
userid int references users(userid),
movieid int references movies(movieid),
rating numeric CHECK(-1<rating AND rating<6),
timestamp bigint not null DEFAULT(extract(epoch from now())*1000),
primary key(userid, movieid)
);

create table tags(
userid int references users(userid),
movieid int references movies(movieid),
tagid int references taginfo(tagid),
timestamp bigint not null DEFAULT(extract(epoch from now())*1000),
primary key(userid, movieid, tagid)
);

create table hasagenre(
movieid int references movies(movieid),
genreid int references genres(genreid),
primary key(movieid, genreid)
);

