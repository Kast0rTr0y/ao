CREATE TABLE person (
    id NUMBER NOT NULL,
    firstName VARCHAR(255) NOT NULL,
    lastName CLOB,
    age NUMBER(12),
    url VARCHAR(255) UNIQUE NOT NULL,
    favoriteClass VARCHAR(255),
    height NUMBER(32,6) DEFAULT 62.3,
    companyID NUMBER,
    cool NUMBER(1) DEFAULT 1,
    modified TIMESTAMP DEFAULT SYSDATE,
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

CREATE TRIGGER person_modified_onupdate
BEFORE UPDATE
    ON person
    FOR EACH ROW
BEGIN
    :NEW.modified := SYSDATE;
END;