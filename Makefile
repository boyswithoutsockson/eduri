# This makefile contains recipes for constructing the database.

SHELL := /bin/bash
.ONESHELL:

.PHONY: help
help: ## show help message
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make <command> \033[36m\033[0m\n"} /^[$$()% a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

# Default for `make` without any args
all: help


####################################
# Scripts for downloading raw data #
####################################

data/dump.zip:
	mkdir -p data/raw
	mkdir -p data/preprocessed
	FILE_ID=1cQb23nkz-DAlo33cU96BnPjdnXU9MoFA
	curl -L "https://drive.usercontent.google.com/download?id=$${FILE_ID}&confirm=true" --progress-bar \
		-o $@

DATA_DUMP = data/.unzipped
$(DATA_DUMP): data/dump.zip
	@touch $@
	unzip -oq data/dump.zip -d data/raw


frontend/src/assets/photos-2023-2026.zip:
	mkdir -p frontend/src/assets
	FILE_ID=1K0ykFwVEdU-EmwSPC_p6Yx-S4Ko5Bylh
	curl -L "https://drive.usercontent.google.com/download?id=$${FILE_ID}&confirm=true" --progress-bar \
		-o $@

MP_PHOTOS = frontend/src/assets/.unzipped
$(MP_PHOTOS): frontend/src/assets/photos-2023-2026.zip
	@touch $@
	unzip -oq frontend/src/assets/photos-2023-2026.zip -d frontend/src/assets

.PHONY: data
data: $(DATA_DUMP) $(MP_PHOTOS) ## download and extract all raw data assets

.PHONY: clean-data
clean: ## deletes all raw data assets
	rm -rf data/.[!.]*
	rm -f frontend/src/assets/.[!.]*


##################################
# Scripts for data preprocessing #
##################################

data/preprocessed/members_of_parliament.csv: pipes/mp_pipe.py $(DATA_DUMP) $(MP_PHOTOS)
	uv run pipes/mp_pipe.py --preprocess-data

data/preprocessed/interests.csv: pipes/interest_pipe.py $(DATA_DUMP)
	uv run pipes/interest_pipe.py --preprocess-data

data/preprocessed/ballots.csv: pipes/ballot_pipe.py $(DATA_DUMP)
	uv run pipes/ballot_pipe.py --preprocess-data

data/preprocessed/votes.csv: pipes/vote_pipe.py $(DATA_DUMP)
	uv run pipes/vote_pipe.py --preprocess-data

data/preprocessed/parties.csv: pipes/parties_pipe.py $(DATA_DUMP)
	uv run pipes/parties_pipe.py --preprocess-data

data/preprocessed/mp_party_memberships.csv: pipes/mp_party_membership_pipe.py $(DATA_DUMP)
	uv run pipes/mp_party_membership_pipe.py --preprocess-data

data/preprocessed/committees.csv: pipes/committee_pipe.py $(DATA_DUMP)
	uv run pipes/committee_pipe.py --preprocess-data

data/preprocessed/mp_committee_memberships.csv: pipes/mp_committee_membership_pipe.py $(DATA_DUMP)
	uv run pipes/mp_committee_membership_pipe.py --preprocess-data


#################################
# Scripts for database creation #
#################################

.PHONY: nuke
nuke: ## resets all data in the database
	PGPASSWORD=postgres psql -q -U postgres -h db postgres < DELETE_ALL_TABLES.sql
	PGPASSWORD=postgres psql -q -U postgres -h db postgres < postgres-init-scripts/01_create_tables.sql
	rm -f data/.inserted

PREPROCESSED_FILES = data/preprocessed/members_of_parliament.csv \
    data/preprocessed/interests.csv \
    data/preprocessed/ballots.csv \
    data/preprocessed/votes.csv \
    data/preprocessed/parties.csv \
    data/preprocessed/mp_party_memberships.csv \
    data/preprocessed/committees.csv \
    data/preprocessed/mp_committee_memberships.csv

DATABASE = data/.inserted
$(DATABASE): $(PREPROCESSED_FILES)
	@touch $@
	@bash -c '\
	TIMEFORMAT="Finished in %3R seconds."; \
	for script in \
		pipes/mp_pipe.py \
		pipes/interest_pipe.py \
		pipes/ballot_pipe.py \
		pipes/vote_pipe.py \
		pipes/parties_pipe.py \
		pipes/mp_party_membership_pipe.py \
		pipes/committee_pipe.py \
		pipes/mp_committee_membership_pipe.py;
	do \
		echo "Importing data with $$script"; \
		time uv run $$script --import-data; \
	done'

.PHONY: database
database: $(DATABASE) ## runs all data pipelines into the database


###############################
# Frontend management scripts #
###############################

.PHONY: install
install:
	cd frontend
	npm install

.PHONY: frontend
frontend:
	cd frontend
	npm run dev
