CREATE TABLE public."person" (
    "id" SERIAL NOT NULL,
    "firstName" VARCHAR(255) NOT NULL,
    "lastName" TEXT,
    "age" INTEGER,
    "url" VARCHAR(767) CONSTRAINT U_person_url UNIQUE NOT NULL,
    "height" DOUBLE PRECISION DEFAULT 62.3,
    "companyID" BIGINT,
    "cool" BOOLEAN DEFAULT TRUE,
    "modified" TIMESTAMP,
    "weight" DOUBLE PRECISION,
    "typeOfPerson" VARCHAR(30),
    CONSTRAINT "fk_person_companyid" FOREIGN KEY ("companyID") REFERENCES public."company"("id"),
    PRIMARY KEY("id")
)

