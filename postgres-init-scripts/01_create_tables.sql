-- Members of Parliament
CREATE TABLE IF NOT EXISTS members_of_parliament (
    id INT PRIMARY KEY NOT NULL,
    first_name VARCHAR(200),
    last_name VARCHAR(200),
    full_name VARCHAR(200), 
    minister BOOLEAN,
    phone_number VARCHAR(50), 
    email VARCHAR(200), 
    occupation VARCHAR(200), 
    year_of_birth INT, 
    place_of_birth VARCHAR(200), 
    place_of_residence VARCHAR(200), 
    constituency VARCHAR(200),
    photo VARCHAR(100)
);


-- Interests (sidonnaisuudet)
CREATE TABLE IF NOT EXISTS interests (
    id SERIAL PRIMARY KEY NOT NULL,
    mp_id INT, 
    category VARCHAR(200), 
    interest TEXT
);

/*
-- Agenda items (kokouskohdat)
CREATE TABLE IF NOT EXISTS agenda_items (
    id INT PRIMARY KEY,
    title VARCHAR(1000),
    start_time TIMESTAMP WITH TIME ZONE,
    session VARCHAR(10),
    sequence INT,
    number FLOAT
);
*/

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


-- Parties (puolueet)
CREATE TABLE IF NOT EXISTS parties (
    id VARCHAR(100) PRIMARY KEY,
    name VARCHAR(100)
);

-- mp_party_memberships
CREATE TABLE IF NOT EXISTS mp_party_memberships (
    party_id VARCHAR(100) REFERENCES parties(id),
    mp_id INT REFERENCES members_of_parliament(id),
    start_date DATE,
    end_date DATE,
    PRIMARY KEY(party_id, mp_id, start_date)
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

-- speeches
CREATE TABLE IF NOT EXISTS speeches (
    id INT PRIMARY KEY NOT NULL,
    mp_id INT NOT NULL REFERENCES members_of_parliament(id),
    parliament_id VARCHAR (20) NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    speech TEXT NOT NULL
);