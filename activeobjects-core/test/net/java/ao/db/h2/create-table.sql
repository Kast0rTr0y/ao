CREATE TABLE PUBLIC.person (
    id INT AUTO_INCREMENT NOT NULL,
    firstName VARCHAR(255) NOT NULL,
    lastName CLOB,
    age INT,
    url VARCHAR(450) NOT NULL,
    height DOUBLE DEFAULT 62.3,
    companyID BIGINT,
    cool BOOLEAN DEFAULT 1,
    modified TIMESTAMP,
    weight DOUBLE,
    typeOfPerson VARCHAR(30),
    CONSTRAINT fk_person_companyid FOREIGN KEY (companyID) REFERENCES PUBLIC.company(id),
 CONSTRAINT U_person_url UNIQUE(url),
    PRIMARY KEY(id)
)
