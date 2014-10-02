CREATE TABLE person (
    id INTEGER AUTO_INCREMENT NOT NULL,
    firstName VARCHAR(255) NOT NULL,
    lastName LONGTEXT,
    age INTEGER,
    url VARCHAR(767) NOT NULL,
    height DOUBLE DEFAULT 62.3,
    companyID BIGINT,
    cool BOOLEAN DEFAULT 1,
    modified DATETIME,
    weight DOUBLE,
    typeOfPerson VARCHAR(30),
    CONSTRAINT fk_person_companyid FOREIGN KEY (companyID) REFERENCES company(id),
 CONSTRAINT U_person_url UNIQUE(url),
    PRIMARY KEY(id)
) ENGINE=InnoDB