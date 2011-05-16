CREATE TABLE person (
    id INTEGER IDENTITY(1,1) NOT NULL,
    firstName VARCHAR(255) NOT NULL,
    lastName NTEXT,
    age INTEGER,
    url VARCHAR(255) UNIQUE NOT NULL,
    favoriteClass VARCHAR(255),
    height DECIMAL(32,6) DEFAULT 62.3,
    companyID BIGINT,
    cool BIT DEFAULT 1,
    modified DATETIME DEFAULT GetDate(),
    weight DECIMAL(32,16),
    CONSTRAINT fk_person_companyid FOREIGN KEY (companyID) REFERENCES company(id),
    PRIMARY KEY(id)
)

CREATE TRIGGER person_modified_onupdate
ON person
FOR UPDATE
AS
    UPDATE person SET modified = GetDate() WHERE id = (SELECT id FROM inserted)