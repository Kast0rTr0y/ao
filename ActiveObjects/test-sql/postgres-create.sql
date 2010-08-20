create database ${db.name};
create user ${jdbc.username} with password '${jdbc.password}';
grant all privileges on database ${db.name} to ${jdbc.username};