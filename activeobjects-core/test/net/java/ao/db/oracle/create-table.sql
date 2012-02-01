CREATE TABLE person (
    id NUMBER(11) NOT NULL,
    firstName VARCHAR(255) NOT NULL,
    lastName CLOB,
    age NUMBER(11),
    url VARCHAR(767) CONSTRAINT U_person_url UNIQUE NOT NULL,
    height DOUBLE PRECISION DEFAULT 62.3,
    companyID NUMBER(20),
    cool NUMBER(1) DEFAULT 1,
    modified TIMESTAMP,
    weight DOUBLE PRECISION,
    typeOfPerson VARCHAR(30),
    CONSTRAINT fk_person_companyid FOREIGN KEY (companyID) REFERENCES company(id),
    PRIMARY KEY(id)
)

CREATE SEQUENCE person_id_SEQ INCREMENT BY 1 START WITH 1 NOMAXVALUE MINVALUE 1

CREATE TRIGGER person_id_autoinc
BEFORE INSERT
    ON person   FOR EACH ROW
BEGIN
    SELECT person_id_SEQ.NEXTVAL INTO :NEW.id FROM DUAL;
END;
