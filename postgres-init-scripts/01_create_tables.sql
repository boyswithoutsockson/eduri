-- Persons
-- Mainly members of parliament
CREATE TABLE IF NOT EXISTS persons (
    id INTEGER PRIMARY KEY NOT NULL,
    first_name TEXT,
    last_name TEXT,
    full_name TEXT, 
    phone_number TEXT, 
    email TEXT, 
    occupation TEXT, 
    year_of_birth INTEGER, 
    place_of_birth TEXT, 
    place_of_residence TEXT, 
    photo TEXT      -- stored as the name of the file (e.g. Zyskowicz-Ben-web-301.jpg)
);

-- Minister positions (ministerisalkku)
-- Different possible minister positions (e.g. Valtiovarainministeri)
CREATE TABLE IF NOT EXISTS minister_positions (
    title TEXT PRIMARY KEY
);

-- Ministers (edustajan ministeriys)
-- Junction table between persons and minister positions
-- Expresses who has held what minister position at what time
CREATE TABLE IF NOT EXISTS ministers (
    person_id INTEGER REFERENCES persons(id),
    minister_position TEXT REFERENCES minister_positions(title), 
    cabinet_id TEXT,     -- Cabinet (hallitus) served. E.g. Lipponen II
    start_date TEXT,
    end_date TEXT,
    PRIMARY KEY(minister_position, person_id, start_date)
);

-- Interests (sidonnaisuudet)
-- Interest or affiliation of an MP. E.g. gifts, stock or other incomes.
CREATE TABLE IF NOT EXISTS interests (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    person_id  INTEGER REFERENCES persons(id), 
    category TEXT, 
    interest TEXT
);

-- Ballots (äänestykset)
CREATE TABLE IF NOT EXISTS ballots (
    id INTEGER PRIMARY KEY NOT NULL,
    title TEXT,
    session_item_title TEXT,   -- (istunnon kohdan otsikko)
    start_time TEXT,
    parliament_id TEXT,  
    minutes_url TEXT,   -- pathname leading to the minutes (pöytäkirja). E.g. /valtiopaivaasiakirjat/PTK+112/1996.
    results_url TEXT    -- same as above for ballot results
);                              -- the root is https://www.eduskunta.fi/FI/vaski

-- Votes (äänet)
-- Junction table between person and ballot to illustrate a single cast vote.
CREATE TABLE IF NOT EXISTS votes (
    ballot_id INTEGER REFERENCES ballots(id),
    person_id INTEGER REFERENCES persons(id),
    vote TEXT, -- ENUM ('yes', 'no', 'abstain', 'absent')
    PRIMARY KEY(ballot_id, person_id)
);

-- Parliamentary groups (eduskuntaryhmät)
CREATE TABLE IF NOT EXISTS parliamentary_groups (
    id TEXT PRIMARY KEY,
    name TEXT
);

-- Mp parliamentary group memberships
CREATE TABLE IF NOT EXISTS mp_parliamentary_group_memberships (
    pg_id TEXT REFERENCES parliamentary_groups(id),
    person_id INTEGER REFERENCES persons(id),
    start_date TEXT,
    end_date TEXT,
    PRIMARY KEY(pg_id, person_id, start_date)
);

-- Assemblies (kokoonpano)
-- Different kinds of assemblies that gather within the parliament.
-- Includes committees (valiokunta) and other groups such as 
-- sectors (jaosto) as well as the general assembly 
CREATE TABLE IF NOT EXISTS assemblies (
    code TEXT UNIQUE,
    name TEXT PRIMARY KEY NOT NULL
);


-- Mp committee memberships
CREATE TABLE IF NOT EXISTS mp_committee_memberships (
    person_id INTEGER NOT NULL,
    committee_name TEXT NOT NULL REFERENCES assemblies(name),
    start_date TEXT NOT NULL,
    end_date TEXT,
    role TEXT NOT NULL,
    PRIMARY KEY(person_id, committee_name, start_date, role)
);

-- Records (pöytäkirjat)
-- Expresses a record of an assembly (valiokunta, eduskunta jne.)
CREATE TABLE IF NOT EXISTS records (
    assembly_code TEXT REFERENCES assemblies(code), -- code for the committee, for instance. E.g. "PuV" for Puolustusvaliokunta
    number INTEGER NOT NULL,
    year INTEGER NOT NULL,
    meeting_date TEXT NOT NULL,
    creation_date TEXT NOT NULL,
    rollcall_id TEXT,        -- only for parliament general assemblies
    PRIMARY KEY(assembly_code, number, year)
);

-- Absences
-- Absence of an assembly's member
-- Identified by the meeting record
CREATE TABLE IF NOT EXISTS absences (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    person_id INTEGER NOT NULL REFERENCES persons(id),
    record_assembly_code TEXT NOT NULL,
    record_number INTEGER NOT NULL,
    record_year INTEGER NOT NULL,
    work_related INTEGER,    -- True if the reason for absence was reported as work related
    FOREIGN KEY (record_assembly_code,
                 record_number,
                 record_year) REFERENCES records(assembly_code,
                                                number,
                                                year)
);

-- Agenda items (asiakohdat)
-- Item on a record
CREATE TABLE IF NOT EXISTS agenda_items (
    parliament_id TEXT,
    record_assembly_code TEXT,
    record_number INTEGER NOT NULL,
    record_year INTEGER NOT NULL,
    title TEXT NOT NULL,
    FOREIGN KEY(record_assembly_code, record_number, record_year) REFERENCES records(assembly_code, number, year),
    PRIMARY KEY(parliament_id, record_assembly_code, record_number, record_year)
);

-- Speeches (puhneenvuorot)
CREATE TABLE IF NOT EXISTS speeches (
    id TEXT PRIMARY KEY NOT NULL,
    person_id INTEGER NOT NULL REFERENCES persons(id),
    record_assembly_code TEXT,
    record_number INTEGER,
    record_year INTEGER,
    agenda_item_parliament_id TEXT,
    start_time TEXT NOT NULL,
    speech TEXT NOT NULL,
    speech_type TEXT NOT NULL,
    response_to TEXT REFERENCES speeches(id),
    FOREIGN KEY (record_assembly_code,
                 record_number,
                 record_year,
                 agenda_item_parliament_id) REFERENCES agenda_items(record_assembly_code,
                                                                    record_number,
                                                                    record_year,
                                                                    parliament_id)
);

-- Proposals (esitykset)
CREATE TABLE IF NOT EXISTS proposals (
    id TEXT PRIMARY KEY NOT NULL,
    ptype TEXT, -- ENUM ('government', 'citizen', 'mp_law', 'mp_petition', 'mp_debate')
    date TEXT NOT NULL,
    title TEXT,
    summary TEXT,
    reasoning TEXT,
    law_changes TEXT,
    status TEXT NOT NULL -- ENUM ('open', 'handled', 'expired', 'cancelled', 'rejected', 'resting', 'passed', 'passed_changed', 'passed_urgent', 'replied', 'dealt')
);

-- Proposal signatures (esitysten allekirjoitukset)
CREATE TABLE IF NOT EXISTS proposal_signatures (
    proposal_id TEXT REFERENCES proposals(id),
    person_id INTEGER REFERENCES persons(id),
    first INTEGER,      -- First signature denotes the creator of the proposal. Government proposals have no first signer.
    PRIMARY KEY(proposal_id, person_id)
);

-- Topics (aiheet)
-- Topic terms, that Vaski data uses to convey topics relevant to a proposal, report etc.
CREATE TABLE IF NOT EXISTS topics (
    topic_id TEXT PRIMARY KEY NOT NULL, -- tail of the URI used as id in finto api and vaski (e.g. p040794)
    term TEXT
);

-- Committee reports (valiokuntien lausunnot)
CREATE TABLE IF NOT EXISTS committee_reports (
    id TEXT PRIMARY KEY NOT NULL,
    proposal_id TEXT NOT NULL,
    date TEXT NOT NULL,
    committee_name TEXT NOT NULL REFERENCES assemblies(name),
    proposal_summary TEXT NOT NULL,
    opinion TEXT NOT NULL,
    reasoning TEXT,
    law_changes TEXT
);

-- Committee budget reports (valiokuntien lausunnot talousesityksiin)
CREATE TABLE IF NOT EXISTS committee_budget_reports (
    id TEXT PRIMARY KEY NOT NULL,
    proposal_id TEXT NOT NULL,
    committee_name TEXT NOT NULL REFERENCES assemblies(name)
);

-- Committee report signatures (valiokuntien lausuntojen allekirjoitukset)
CREATE TABLE IF NOT EXISTS committee_report_signatures (
    committee_report_id TEXT REFERENCES committee_reports(id),
    person_id INTEGER REFERENCES persons(id),
    PRIMARY KEY(committee_report_id, person_id)
);

-- Objections (vastalauseet)
CREATE TABLE IF NOT EXISTS objections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    committee_report_id TEXT REFERENCES committee_reports(id),
    reasoning TEXT,
    motion TEXT
);

-- Objection signatures (vastalauseiden allekirjoitusket)
CREATE TABLE IF NOT EXISTS objection_signatures (
    objection_id INTEGER REFERENCES objections(id),
    person_id INTEGER REFERENCES persons(id),
    PRIMARY KEY(objection_id, person_id)
);

-- Interpellations (välikysymykset)
-- Inquiry meant to question the confidence in government
CREATE TABLE IF NOT EXISTS interpellations (
    id TEXT PRIMARY KEY NOT NULL,
    date TEXT NOT NULL,
    title TEXT NOT NULL,
    reasoning TEXT,
    motion TEXT,
    status TEXT
);

-- Interpellation signatures (välikysymysten allekirjoitukset)
CREATE TABLE IF NOT EXISTS interpellation_signatures (
    interpellation_id TEXT NOT NULL REFERENCES interpellations(id),
    person_id INTEGER REFERENCES persons(id),
    first INTEGER,
    PRIMARY KEY(interpellation_id, person_id)
);

-- Election seasons
-- the active terms of parliaments, for example 
-- for the parliament of 2019-2023: 2019-04-17, 2023-04-04
CREATE TABLE IF NOT EXISTS election_seasons (
    start_date TEXT PRIMARY KEY,
    end_date TEXT
);

-- Lobbies (lobbaajat)
-- Entities doing the lobbying. Companies or other communities.
CREATE TABLE IF NOT EXISTS lobbies (
    id TEXT PRIMARY KEY NOT NULL, -- Y-tunnus for Finnish entities
    name TEXT NOT NULL,
    industry TEXT
);

-- Lobby topics
-- Whenever a lobby is contacting someone, they have to state the relating topic.
CREATE TABLE IF NOT EXISTS lobby_topics (
    id INTEGER PRIMARY KEY NOT NULL,
    topic TEXT NOT NULL,
    project TEXT     -- Sometimes topics are attached to a government project (hanke)
);

-- Lobby terms 
-- Lobby reporting is done in half a year long terms.
CREATE TABLE IF NOT EXISTS lobby_terms (    
    id INTEGER PRIMARY KEY NOT NULL,
    start_date TEXT,
    end_date TEXT
);

-- Lobby actions
-- Lobby action expresses a communication instance between a lobby and a person
CREATE TABLE IF NOT EXISTS lobby_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    term_id INTEGER NOT NULL REFERENCES lobby_terms(id),
    lobby_id TEXT NOT NULL REFERENCES lobbies(id),
    person_id INTEGER NOT NULL REFERENCES persons(id),
    topic_id INTEGER NOT NULL REFERENCES lobby_topics(id),
    contact_method TEXT
);

-- Election fundings
-- From Valtiontalouden tarkastusvirasto
CREATE TABLE IF NOT EXISTS election_fundings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id INTEGER NOT NULL REFERENCES persons(id),
    election_year INTEGER NOT NULL,
    ftype TEXT NOT NULL, -- ENUM ('loan', 'person', 'company', 'party', 'party_union', 'other', 'forwarded')
    funder_organization TEXT,
    funder_company_id TEXT,
    funder_first_name TEXT,
    funder_last_name TEXT,
    loan_title TEXT,
    loan_schedule TEXT,
    amount REAL NOT NULL
);

-- Election budgets
-- Election budgets From Valtiontalouden tarkastusvirasto
CREATE TABLE IF NOT EXISTS election_budgets (
    person_id INTEGER NOT NULL REFERENCES persons(id),
    support_group TEXT,     -- Candidates often have organizations specifically for their campaign
    election_year INTEGER NOT NULL,
    expenses_total REAL NOT NULL,
    incomes_total REAL NOT NULL,
    income_own REAL NOT NULL,       -- Different types of incomes
    income_loan REAL NOT NULL,
    income_person REAL NOT NULL,
    income_company REAL NOT NULL,
    income_party REAL NOT NULL,
    income_party_union REAL NOT NULL,
    income_forwarded REAL NOT NULL,
    income_other REAL NOT NULL,
    PRIMARY KEY(person_id, election_year)
);

-- Promises
-- Election promise from Yle Vaalikone
CREATE TABLE IF NOT EXISTS promises (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id INTEGER NOT NULL REFERENCES persons(id),
    promise TEXT NOT NULL,
    election_year INTEGER NOT NULL
);

