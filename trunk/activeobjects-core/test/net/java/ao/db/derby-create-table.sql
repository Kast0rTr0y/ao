CREATE TABLE person (
    id INTEGER GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    firstName VARCHAR(255) NOT NULL,
    lastName CLOB,
    age INTEGER,
    url VARCHAR(255) UNIQUE NOT NULL,
    favoriteClass VARCHAR(255),
    height DOUBLE DEFAULT 62.3,
    companyID BIGINT,
    cool SMALLINT DEFAULT 1,
    modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_person_companyid FOREIGN KEY (companyID) REFERENCES company(id),
    PRIMARY KEY(id)
)

CREATE TRIGGER person_modified_onupdate
    AFTER UPDATE ON person
    REFERENCING NEW AS inserted
    FOR EACH ROW MODE DB2SQL
        UPDATE person SET modified = CURRENT_TIMESTAMP
            WHERE id = inserted.id AND inserted.modified <> CURRENT_TIMESTAMP