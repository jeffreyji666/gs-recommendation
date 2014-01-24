create database gs_recommendation;

use gs_recommendation;

CREATE table IF NOT EXISTS recommend_item(
    id integer primary key auto_increment,
    url varchar(256) not null,
    name varchar(50) not null,
    description varchar(100),
    created datetime,
    modified datetime
)ENGINE=InnoDB  DEFAULT CHARSET=utf8;
