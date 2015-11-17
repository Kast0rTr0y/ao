CREATE TABLE person (
    id INTEGER GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    firstName VARCHAR(255) NOT NULL,
    lastName TEXT,
    age INTEGER,
    url VARCHAR(450) UNIQUE NOT NULL,
    height DOUBLE DEFAULT 62.3,
    companyID BIGINT,
    cool BOOLEAN DEFAULT 1,
    modified TIMESTAMP,
    weight DOUBLE,
    typeOfPerson VARCHAR(30),
    FOREIGN KEY (companyID) REFERENCES company(id),
    PRIMARY KEY(id)
)