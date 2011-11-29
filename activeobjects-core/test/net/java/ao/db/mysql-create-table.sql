CREATE TABLE person (
    id INTEGER AUTO_INCREMENT NOT NULL,
    firstName VARCHAR(255) NOT NULL,
    lastName TEXT,
    age INTEGER(12),
    url VARCHAR(255) NOT NULL,
    favoriteClass VARCHAR(255),
    height DOUBLE(32,6) DEFAULT 62.3,
    companyID BIGINT,
    cool BOOLEAN DEFAULT 1,
    modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    weight DOUBLE(32,16),
    CONSTRAINT fk_person_companyid FOREIGN KEY (companyID) REFERENCES company(id),
 CONSTRAINT U_person_url UNIQUE(url),
    PRIMARY KEY(id)
) ENGINE=InnoDB