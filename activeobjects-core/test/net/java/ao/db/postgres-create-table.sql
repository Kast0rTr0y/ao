CREATE TABLE public."person" (
    "id" SERIAL NOT NULL,
    "firstName" VARCHAR(255) NOT NULL,
    "lastName" TEXT,
    "age" INTEGER,
    "url" VARCHAR(255) CONSTRAINT U_person_url UNIQUE NOT NULL,
    "favoriteClass" VARCHAR(255),
    "height" DOUBLE PRECISION DEFAULT 62.3,
    "companyID" BIGINT,
    "cool" BOOLEAN DEFAULT TRUE,
    "modified" TIMESTAMP DEFAULT now(),
    "weight" DOUBLE PRECISION,
    CONSTRAINT "fk_person_companyid" FOREIGN KEY ("companyID") REFERENCES public."company"("id"),
    PRIMARY KEY("id")
)

CREATE FUNCTION public.person_modified_onupdate() RETURNS trigger AS $person_modified_onupdate$
BEGIN
    NEW."modified" := now();
    RETURN NEW;
END;
$person_modified_onupdate$ LANGUAGE plpgsql

CREATE TRIGGER person_modified_onupdate
 BEFORE UPDATE OR INSERT ON public."person"
    FOR EACH ROW EXECUTE PROCEDURE public.person_modified_onupdate()