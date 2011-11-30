CREATE TABLE PUBLIC.person (
    id INTEGER GENERATED BY DEFAULT AS IDENTITY (START WITH 1) NOT NULL,
    firstName VARCHAR(255) NOT NULL,
    lastName LONGVARCHAR,
    age INTEGER,
    url VARCHAR(255) NOT NULL,
    height DOUBLE DEFAULT 62.3,
    companyID BIGINT,
    cool BOOLEAN DEFAULT TRUE,
    modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    weight DOUBLE,
    CONSTRAINT fk_person_companyid FOREIGN KEY (companyID) REFERENCES PUBLIC.company(id),
 CONSTRAINT U_person_url UNIQUE(url),
    PRIMARY KEY(id)
)