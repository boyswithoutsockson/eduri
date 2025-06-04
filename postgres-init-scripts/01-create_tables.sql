-- Members of Parliament
CREATE TABLE IF NOT EXISTS members_of_parliament (
    id INT PRIMARY KEY NOT NULL,
    first_name VARCHAR(200), 
    full_name VARCHAR(200),
    party VARCHAR(4), 
    minister BOOLEAN, 
    phone_number VARCHAR(50), 
    email VARCHAR(200), 
    occupation VARCHAR(200), 
    year_of_birth INT, 
    place_of_birth VARCHAR(200), 
    place_of_residence VARCHAR(200), 
    constituency VARCHAR(200)
);


-- Interests (sidonnaisuudet)
CREATE TABLE IF NOT EXISTS interests (
    id INT PRIMARY KEY NOT NULL,
    mp_id INT, 
    category VARCHAR(200), 
    interest TEXT
);


-- Agenda items (kokouskohdat)
CREATE TABLE IF NOT EXISTS agenda_items (
    id INT PRIMARY KEY,
    title VARCHAR(500),
    time TIMESTAMP WITH TIME ZONE,
    url VARCHAR(200)
);

-- Ballots (äänestykset)
CREATE TABLE IF NOT EXISTS ballots (
    id INT PRIMARY KEY NOT NULL,
    agenda_item_id INT REFERENCES agenda_items(id), 
    title VARCHAR(500),
    time TIMESTAMP WITH TIME ZONE,
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
    id VARCHAR(10) PRIMARY KEY,
    name VARCHAR(100)
);

-- mp_party_memberships
CREATE TABLE IF NOT EXISTS mp_party_memberships (
    party_id VARCHAR(10) REFERENCES parties(id),
    mp_id INT REFERENCES members_of_parliament(id),
    start_time TIMESTAMP WITH TIME ZONE,
    end_time TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY(party_id, mp_id, start_time)
);

