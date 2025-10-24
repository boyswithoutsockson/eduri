-- Members of Parliament
CREATE TABLE IF NOT EXISTS persons (
    id INT PRIMARY KEY NOT NULL,
    first_name VARCHAR(200),
    last_name VARCHAR(200),
    full_name VARCHAR(200), 
    phone_number VARCHAR(50), 
    email VARCHAR(200), 
    occupation VARCHAR(200), 
    year_of_birth INT, 
    place_of_birth VARCHAR(200), 
    place_of_residence VARCHAR(200), 
    photo VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS minister_positions (
    title VARCHAR(100) PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS ministers (
    person_id INT REFERENCES persons(id),
    minister_position VARCHAR(100) REFERENCES minister_positions(title), 
    cabinet_id VARCHAR(50), 
    start_date DATE,
    end_date DATE,
    PRIMARY KEY(minister_position, person_id, start_date)
);

-- Interests (sidonnaisuudet)
CREATE TABLE IF NOT EXISTS interests (
    id SERIAL PRIMARY KEY NOT NULL,
    person_id  INT REFERENCES persons(id), 
    category VARCHAR(200), 
    interest TEXT
);

-- Ballots (äänestykset)
CREATE TABLE IF NOT EXISTS ballots (
    id INT PRIMARY KEY NOT NULL,
    title VARCHAR(500),
    session_item_title VARCHAR(2000),
    start_time TIMESTAMP WITH TIME ZONE,
    parliament_id VARCHAR(50),
    minutes_url VARCHAR(200),
    results_url VARCHAR(200)
);

DO $$ BEGIN  
    CREATE TYPE vote AS ENUM ('yes', 'no', 'abstain', 'absent');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- Votes (äänet)
CREATE TABLE IF NOT EXISTS votes (
    ballot_id INT REFERENCES ballots(id),
    person_id INT REFERENCES persons(id),
    vote vote,
    PRIMARY KEY(ballot_id, person_id)
);

-- parliamentary_groups (puolueet)
CREATE TABLE IF NOT EXISTS parliamentary_groups (
    id VARCHAR(100) PRIMARY KEY,
    name VARCHAR(100)
);

-- mp_parliamentary_group_memberships
CREATE TABLE IF NOT EXISTS mp_parliamentary_group_memberships (
    pg_id VARCHAR(100) REFERENCES parliamentary_groups(id),
    person_id INT REFERENCES persons(id),
    start_date DATE,
    end_date DATE,
    PRIMARY KEY(pg_id, person_id, start_date)
);

-- committees (valiokunnat)
CREATE TABLE IF NOT EXISTS committees (
    name VARCHAR(200) PRIMARY KEY
);

-- mp_committee_memberships
CREATE TABLE IF NOT EXISTS mp_committee_memberships (
    person_id INT NOT NULL,
    committee_name VARCHAR(200) NOT NULL REFERENCES committees(name),
    start_date DATE NOT NULL,
    end_date DATE,
    role VARCHAR(50) NOT NULL,
    PRIMARY KEY(person_id, committee_name, start_date, role)
);

-- assemblies
CREATE TABLE IF NOT EXISTS assemblies (
    code VARCHAR(10) PRIMARY KEY NOT NULL,
    name VARCHAR(50) NOT NULL
);

-- sessions

CREATE TABLE IF NOT EXISTS records (
    assembly_code VARCHAR(10) REFERENCES assemblies(code),
    nro INT NOT NULL,
    year INT NOT NULL,
    meeting_date DATE NOT NULL,
    creation_date DATE NOT NULL,
    PRIMARY KEY(assembly_code, nro, year)
);

-- agenda items
CREATE TABLE IF NOT EXISTS agenda_items (
    parliament_id VARCHAR (20),
    record_assembly_code VARCHAR(10),
    record_nro INT NOT NULL,
    record_year INT NOT NULL,
    title TEXT NOT NULL,
    FOREIGN KEY(record_assembly_code, record_nro, record_year) REFERENCES records(assembly_code, nro, year),
    PRIMARY KEY(parliament_id, record_assembly_code, record_nro, record_year)
);

-- speeches
CREATE TABLE IF NOT EXISTS speeches (
    id VARCHAR(15) PRIMARY KEY NOT NULL,
    person_id INT NOT NULL REFERENCES persons(id),
    record_assembly_code VARCHAR (10),
    record_nro INT,
    record_year INT,
    agenda_item_parliament_id VARCHAR (20),
    FOREIGN KEY (record_assembly_code,
                 record_nro,
                 record_year,
                 agenda_item_parliament_id) REFERENCES agenda_items(record_assembly_code,
                                                                    record_nro,
                                                                    record_year,
                                                                    parliament_id),
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    speech TEXT NOT NULL,
    speech_type CHAR(1) NOT NULL,
    response_to VARCHAR(15) REFERENCES speeches(id)
);

DO $$ BEGIN  
    CREATE TYPE proposal_type AS ENUM ('government', 'citizen', 'mp_law', 'mp_petition', 'mp_debate');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN  
    CREATE TYPE proposal_status AS ENUM ('open', 'expired', 'cancelled', 'rejected', 'resting', 'passed', 'passed_changed', 'passed_urgent');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- proposals
CREATE TABLE IF NOT EXISTS proposals (
    id VARCHAR(20) PRIMARY KEY NOT NULL,
    ptype proposal_type,
    date DATE NOT NULL,
    title VARCHAR(1000),
    summary TEXT,
    reasoning TEXT,
    law_changes TEXT,
    status proposal_status NOT NULL
);

-- proposal_signatures
CREATE TABLE IF NOT EXISTS proposal_signatures (
    proposal_id VARCHAR(20) REFERENCES proposals(id),
    person_id INT REFERENCES persons(id),
    first BOOLEAN,
    PRIMARY KEY(proposal_id, person_id)
);


-- committee_reports
CREATE TABLE IF NOT EXISTS committee_reports (
    id VARCHAR(20) PRIMARY KEY NOT NULL,
    proposal_id VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    committee_name VARCHAR(200) NOT NULL REFERENCES committees(name),
    proposal_summary TEXT NOT NULL,
    opinion TEXT NOT NULL,
    reasoning TEXT,
    law_changes TEXT
);

-- committee_budget_reports
CREATE TABLE IF NOT EXISTS committee_budget_reports (
    id VARCHAR(20) PRIMARY KEY NOT NULL,
    proposal_id VARCHAR(20) NOT NULL,
    committee_name VARCHAR(200) NOT NULL REFERENCES committees(name)
);

-- committee_report_signatures
CREATE TABLE IF NOT EXISTS committee_report_signatures (
    committee_report_id VARCHAR(20) REFERENCES committee_reports(id),
    person_id INT REFERENCES persons(id),
    PRIMARY KEY(committee_report_id, person_id)
);

-- objections
CREATE TABLE IF NOT EXISTS objections (
    id SERIAL PRIMARY KEY,
    committee_report_id VARCHAR(20) REFERENCES committee_reports(id),
    reasoning TEXT,
    motion TEXT
);

-- objection_signatures
CREATE TABLE IF NOT EXISTS objection_signatures (
    objection_id SERIAL REFERENCES objections(id),
    person_id INT REFERENCES persons(id),
    PRIMARY KEY(objection_id, person_id)
);

CREATE TABLE IF NOT EXISTS election_seasons (
    start_year DATE,
    end_year DATE,
    PRIMARY KEY(start_year, end_year)
)

