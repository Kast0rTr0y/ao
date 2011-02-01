CREATE TABLE 'person' (
    'id' SERIAL NOT NULL,
    'firstName' VARCHAR(255) NOT NULL,
    'lastName' TEXT,
    'age' INTEGER,
    'url' VARCHAR(255) UNIQUE NOT NULL,
    'favoriteClass' VARCHAR(255),
    'height' DOUBLE PRECISION DEFAULT 62.3,
    'companyID' BIGINT,
    'cool' BOOLEAN DEFAULT TRUE,
    'modified' TIMESTAMP DEFAULT now(),
    CONSTRAINT 'fk_person_companyid' FOREIGN KEY ('companyID') REFERENCES 'company'('id'),
    PRIMARY KEY('id')
)

CREATE FUNCTION person_modified_onupdate() RETURNS trigger AS $person_modified_onupdate$
BEGIN
    NEW.'modified' := now();
    RETURN NEW;
END;
$person_modified_onupdate$ LANGUAGE plpgsql

CREATE TRIGGER person_modified_onupdate
 BEFORE UPDATE OR INSERT ON 'person'
    FOR EACH ROW EXECUTE PROCEDURE person_modified_onupdate()