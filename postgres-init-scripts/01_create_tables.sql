-- Persons
-- Mainly members of parliament
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
    photo VARCHAR(100)      -- stored as the name of the file (e.g. Zyskowicz-Ben-web-301.jpg)
);

-- Minister positions (ministerisalkku)
-- Different possible minister positions (e.g. Valtiovarainministeri)
CREATE TABLE IF NOT EXISTS minister_positions (
    title VARCHAR(100) PRIMARY KEY
);

-- Ministers (edustajan ministeriys)
-- Junction table between persons and minister positions
-- Expresses who has held what minister position at what time
CREATE TABLE IF NOT EXISTS ministers (
    person_id INT REFERENCES persons(id),
    minister_position VARCHAR(100) REFERENCES minister_positions(title), 
    cabinet_id VARCHAR(50),     -- Cabinet (hallitus) served. E.g. Lipponen II
    start_date DATE,
    end_date DATE,
    PRIMARY KEY(minister_position, person_id, start_date)
);

-- Interests (sidonnaisuudet)
-- Interest or affiliation of an MP. E.g. gifts, stock or other incomes.
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
    session_item_title VARCHAR(2000),   -- (istunnon kohdan otsikko)
    start_time TIMESTAMP WITH TIME ZONE,
    parliament_id VARCHAR(50),  
    minutes_url VARCHAR(200),   -- path name leading to the minutes (pöytäkirja). E.g. /valtiopaivaasiakirjat/PTK+112/1996.
    results_url VARCHAR(200)    -- same as above for ballot results
);                              -- the root is https://www.eduskunta.fi/FI/vaski

-- vote data type
DO $$ BEGIN  
    CREATE TYPE vote AS ENUM ('yes', 'no', 'abstain', 'absent');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- Votes (äänet)
-- Junction table between person and ballot to illustrate a single cast vote.
CREATE TABLE IF NOT EXISTS votes (
    ballot_id INT REFERENCES ballots(id),
    person_id INT REFERENCES persons(id),
    vote vote,
    PRIMARY KEY(ballot_id, person_id)
);

-- Parliamentary groups (eduskuntaryhmät)
CREATE TABLE IF NOT EXISTS parliamentary_groups (
    id VARCHAR(100) PRIMARY KEY,
    name VARCHAR(100)
);

-- Mp parliamentary group memberships
CREATE TABLE IF NOT EXISTS mp_parliamentary_group_memberships (
    pg_id VARCHAR(100) REFERENCES parliamentary_groups(id),
    person_id INT REFERENCES persons(id),
    start_date DATE,
    end_date DATE,
    PRIMARY KEY(pg_id, person_id, start_date)
);

-- Assemblies (kokoonpano)
-- Different kinds of assemblies that gather within the parliament.
-- Includes committees (valiokunta) and other groups such as 
-- sectors (jaosto) as well as the general assembly 
CREATE TABLE IF NOT EXISTS assemblies (
    code VARCHAR(10) UNIQUE,
    name VARCHAR(100) PRIMARY KEY NOT NULL
);


-- Mp committee memberships
CREATE TABLE IF NOT EXISTS mp_committee_memberships (
    person_id INT NOT NULL,
    committee_name VARCHAR(200) NOT NULL REFERENCES assemblies(name),
    start_date DATE NOT NULL,
    end_date DATE,
    role VARCHAR(50) NOT NULL,
    PRIMARY KEY(person_id, committee_name, start_date, role)
);

-- Records (pöytäkirjat)
-- Expresses a record of an assembly (valiokunta, eduskunta jne.)
CREATE TABLE IF NOT EXISTS records (
    assembly_code VARCHAR(10) REFERENCES assemblies(code), -- code for the committee, for instance. E.g. "PuV" for Puolustusvaliokunta
    number INT NOT NULL,
    year INT NOT NULL,
    meeting_date DATE NOT NULL,
    creation_date DATE NOT NULL,
    PRIMARY KEY(assembly_code, number, year)
);

-- Agenda items (asiakohdat)
-- Item on a record
CREATE TABLE IF NOT EXISTS agenda_items (
    parliament_id VARCHAR (20),
    record_assembly_code VARCHAR(10),
    record_number INT NOT NULL,
    record_year INT NOT NULL,
    title TEXT NOT NULL,
    FOREIGN KEY(record_assembly_code, record_number, record_year) REFERENCES records(assembly_code, number, year),
    PRIMARY KEY(parliament_id, record_assembly_code, record_number, record_year)
);

-- Speeches (puhneenvuorot)
CREATE TABLE IF NOT EXISTS speeches (
    id VARCHAR(15) PRIMARY KEY NOT NULL,
    person_id INT NOT NULL REFERENCES persons(id),
    record_assembly_code VARCHAR (10),
    record_number INT,
    record_year INT,
    agenda_item_parliament_id VARCHAR (20),
    FOREIGN KEY (record_assembly_code,
                 record_number,
                 record_year,
                 agenda_item_parliament_id) REFERENCES agenda_items(record_assembly_code,
                                                                    record_number,
                                                                    record_year,
                                                                    parliament_id),
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    speech TEXT NOT NULL,
    speech_type CHAR(1) NOT NULL,
    response_to VARCHAR(15) REFERENCES speeches(id)
);

-- data type for different types of proposals
DO $$ BEGIN  
    CREATE TYPE proposal_type AS ENUM ('government', 'citizen', 'mp_law', 'mp_petition', 'mp_debate');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- data type for different statuses of proposals
DO $$ BEGIN  
    CREATE TYPE proposal_status AS ENUM ('open', 'expired', 'cancelled', 'rejected', 'resting', 'passed', 'passed_changed', 'passed_urgent');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- Proposals (esitykset)
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

-- Proposal signatures (esitysten allekirjoitukset)
CREATE TABLE IF NOT EXISTS proposal_signatures (
    proposal_id VARCHAR(20) REFERENCES proposals(id),
    person_id INT REFERENCES persons(id),
    first BOOLEAN,      -- First signature denotes the creator of the proposal. Government proposals have no first signer.
    PRIMARY KEY(proposal_id, person_id)
);


-- Committee reports (valiokuntien lausunnot)
CREATE TABLE IF NOT EXISTS committee_reports (
    id VARCHAR(20) PRIMARY KEY NOT NULL,
    proposal_id VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    committee_name VARCHAR(200) NOT NULL REFERENCES assemblies(name),
    proposal_summary TEXT NOT NULL,
    opinion TEXT NOT NULL,
    reasoning TEXT,
    law_changes TEXT
);

-- Committee budget reports (valiokuntien lausunnot talousesityksiin)
CREATE TABLE IF NOT EXISTS committee_budget_reports (
    id VARCHAR(20) PRIMARY KEY NOT NULL,
    proposal_id VARCHAR(20) NOT NULL,
    committee_name VARCHAR(200) NOT NULL REFERENCES assemblies(name)
);

-- Committee report signatures (valiokuntien lausuntojen allekirjoitukset)
CREATE TABLE IF NOT EXISTS committee_report_signatures (
    committee_report_id VARCHAR(20) REFERENCES committee_reports(id),
    person_id INT REFERENCES persons(id),
    PRIMARY KEY(committee_report_id, person_id)
);

-- Objections (vastalauseet)
CREATE TABLE IF NOT EXISTS objections (
    id SERIAL PRIMARY KEY,
    committee_report_id VARCHAR(20) REFERENCES committee_reports(id),
    reasoning TEXT,
    motion TEXT
);

-- Objection signatures (vastalauseiden allekirjoitusket)
CREATE TABLE IF NOT EXISTS objection_signatures (
    objection_id SERIAL REFERENCES objections(id),
    person_id INT REFERENCES persons(id),
    PRIMARY KEY(objection_id, person_id)
);

-- Election seasons
-- the active terms of parliaments, for example 
-- for the parliament of 2019-2023: 2019-04-17, 2023-04-04
CREATE TABLE IF NOT EXISTS election_seasons (
    start_date DATE,
    end_date DATE,
    PRIMARY KEY(start_date, end_date)
);

-- Lobbies (lobbaajat)
-- Entities doing the lobbying. Companies or other communities.
CREATE TABLE IF NOT EXISTS lobbies (
    id VARCHAR(20) PRIMARY KEY NOT NULL, -- Y-tunnus for Finnish entities
    name VARCHAR(200) NOT NULL,
    industry VARCHAR(200)
);

-- Lobby topics
-- Whenever a lobby is contacting someone, they have to state the relating topic.
CREATE TABLE IF NOT EXISTS lobby_topics (
    id INT PRIMARY KEY NOT NULL,
    topic VARCHAR(1000) NOT NULL,
    project VARCHAR(20)     -- Sometimes topics are attached to a government project (hanke)
);

-- Lobby terms 
-- Lobby reporting is done in half a year long terms.
CREATE TABLE IF NOT EXISTS lobby_terms (    
    id INT PRIMARY KEY NOT NULL,
    start_date DATE,
    end_date DATE
);

-- Lobby actions
-- Lobby action expresses a communication instance between a lobby and a person
CREATE TABLE IF NOT EXISTS lobby_actions (
    id SERIAL PRIMARY KEY,
    term_id INT NOT NULL REFERENCES lobby_terms(id),
    lobby_id VARCHAR(20) NOT NULL REFERENCES lobbies(id),
    person_id INT NOT NULL REFERENCES persons(id),
    topic_id INT NOT NULL REFERENCES lobby_topics(id),
    contact_method VARCHAR(50)
);


