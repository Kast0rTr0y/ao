CREATE TABLE dbo.person (
    id INTEGER IDENTITY(1,1) NOT NULL,
    firstName NVARCHAR(255) NOT NULL,
    lastName NVARCHAR(max),
    age INTEGER,
    url NVARCHAR(450) CONSTRAINT U_person_url UNIQUE NOT NULL,
    height FLOAT CONSTRAINT df_person_height DEFAULT 62.3,
    companyID BIGINT,
    cool BIT CONSTRAINT df_person_cool DEFAULT 1,
    modified DATETIME,
    weight FLOAT,
    typeOfPerson NVARCHAR(30),
    CONSTRAINT fk_person_companyid FOREIGN KEY (companyID) REFERENCES dbo.company(id),
CONSTRAINT pk_person_id PRIMARY KEY(id)
)
