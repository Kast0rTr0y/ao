ALTER TABLE company ADD eman CLOB

UPDATE company SET eman = name_1

SAVEPOINT values_copied

ALTER TABLE company DROP COLUMN name_1

ALTER TABLE company RENAME COLUMN eman TO name_1
