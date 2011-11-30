CREATE TABLE dbo.person (
    id INTEGER IDENTITY(1,1) NOT NULL,
    firstName VARCHAR(255) NOT NULL,
    lastName NTEXT,
    age INTEGER,
    url VARCHAR(255) CONSTRAINT U_person_url UNIQUE NOT NULL,
    height DECIMAL(32,6) CONSTRAINT df_person_height DEFAULT 62.3,
    companyID BIGINT,
    cool BIT CONSTRAINT df_person_cool DEFAULT 1,
    modified DATETIME CONSTRAINT df_person_modified DEFAULT GetDate(),
    weight DECIMAL(32,16),
    CONSTRAINT fk_person_companyid FOREIGN KEY (companyID) REFERENCES dbo.company(id),
    PRIMARY KEY(id)
)

CREATE TRIGGER person_modified_onupdate
ON dbo.person
FOR UPDATE
AS
    UPDATE dbo.person SET modified = GetDate() WHERE id = (SELECT id FROM inserted)