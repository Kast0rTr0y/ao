create database ${db.name};
create user '${jdbc.username}'@'localhost' identified by '${jdbc.password}';
grant all on ${db.name}.* to '${jdbc.username}'@'localhost';