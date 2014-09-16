ALTER TABLE company ADD eman CLOB

UPDATE company SET eman = name

ALTER TABLE company DROP COLUMN name

ALTER TABLE company RENAME COLUMN eman TO name
