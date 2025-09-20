-- Members of Parliament
CREATE TABLE IF NOT EXISTS members_of_parliament (
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
    constituency VARCHAR(200),
    photo VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS ministers (
    mp_id INT REFERENCES members_of_parliament(id),
    ministry VARCHAR(100), 
    cabinet_id VARCHAR(50), 
    start_date DATE,
    end_date DATE,
    PRIMARY KEY(ministry, mp_id, start_date)
);

-- Interests (sidonnaisuudet)
CREATE TABLE IF NOT EXISTS interests (
    id SERIAL PRIMARY KEY NOT NULL,
    mp_id INT, 
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
    mp_id INT REFERENCES members_of_parliament(id),
    vote vote,
    PRIMARY KEY(ballot_id, mp_id)
);


-- Committees (toimielin)
-- CREATE TABLE IF NOT EXISTS committees (
    
-- );


-- parliamentary_groups (puolueet)
CREATE TABLE IF NOT EXISTS parliamentary_groups (
    id VARCHAR(100) PRIMARY KEY,
    name VARCHAR(100)
);

-- mp_parliamentary_group_memberships
CREATE TABLE IF NOT EXISTS mp_parliamentary_group_memberships (
    pg_id VARCHAR(100) REFERENCES parliamentary_groups(id),
    mp_id INT REFERENCES members_of_parliament(id),
    start_date DATE,
    end_date DATE,
    PRIMARY KEY(pg_id, mp_id, start_date)
);

-- committees (valiokunnat)
CREATE TABLE IF NOT EXISTS committees (
    name VARCHAR(200) PRIMARY KEY
);

-- mp_committee_memberships
CREATE TABLE IF NOT EXISTS mp_committee_memberships (
    mp_id INT NOT NULL,
    committee_name VARCHAR(200) NOT NULL REFERENCES committees(name),
    start_date DATE NOT NULL,
    end_date DATE,
    role VARCHAR(50) NOT NULL,
    PRIMARY KEY(mp_id, committee_name, start_date, role)
);

-- sessions

CREATE TABLE IF NOT EXISTS sessions (
    id VARCHAR(10) PRIMARY KEY NOT NULL,
    date DATE NOT NULL
);

-- agenda items
CREATE TABLE IF NOT EXISTS agenda_items (
    id INT PRIMARY KEY NOT NULL,
    parliament_id VARCHAR (20) NOT NULL,
    session_id VARCHAR(15) REFERENCES sessions(id),
    title TEXT NOT NULL
);

-- speeches
CREATE TABLE IF NOT EXISTS speeches (
    id VARCHAR(15) PRIMARY KEY NOT NULL,
    mp_id INT NOT NULL REFERENCES members_of_parliament(id),
    parliament_id VARCHAR (20) NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    speech TEXT NOT NULL,
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
    title VARCHAR(1000),
    summary TEXT,
    reasoning TEXT NOT NULL,
    law_changes TEXT,
    status proposal_status NOT NULL
);

-- proposal_signatures
CREATE TABLE IF NOT EXISTS proposal_signatures (
    proposal_id VARCHAR(20) REFERENCES proposals(id),
    mp_id INT REFERENCES members_of_parliament(id),
    first BOOLEAN,
    PRIMARY KEY(proposal_id, mp_id)
);


-- committee_reports
CREATE TABLE IF NOT EXISTS committee_reports (
    id VARCHAR(20) PRIMARY KEY NOT NULL,
    proposal_id VARCHAR(20) NOT NULL,
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
    mp_id INT REFERENCES members_of_parliament(id),
    PRIMARY KEY(committee_report_id, mp_id)
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
    mp_id INT REFERENCES members_of_parliament(id),
    PRIMARY KEY(objection_id, mp_id)
);

