# This makefile contains recipes for constructing the database.

SHELL := /bin/bash
.ONESHELL:

# Parallelize execution based on available CPU cores
NPROCS = $(shell grep -c 'processor' /proc/cpuinfo)
MAKEFLAGS += -j$(NPROCS)

# Data pipeline configs
DB = data/.inserted
PREPROCESSED = data/preprocessed
PIPES := \
	agenda_items \
	ballots \
	committee_reports \
	committees \
	election_seasons \
	government_proposals \
	interests \
	ministers \
	mp_committee_memberships \
	mp_law_proposals \
	mp_petition_proposals \
	mp_parliamentary_group_memberships \
	mps \
	parliamentary_groups \
	sessions \
	speeches \
	votes


###################
# Generic scripts #
###################

.PHONY: help
help: ## show help message
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make <command> \033[36m\033[0m\n"} /^[$$()% a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

# Default for `make` without any args
all: help

.PHONY: install
install: install-pipes install-frontend  ## install project dependencies

dbshell: ## connect to the current database
	PGPASSWORD=postgres psql -q -U postgres -h $${DATABASE_HOST:-db} postgres


####################################
# Scripts for downloading raw data #
####################################

data/dump.zip:
	mkdir -p data
	FILE_ID=1cQb23nkz-DAlo33cU96BnPjdnXU9MoFA
	curl -L "https://drive.usercontent.google.com/download?id=$${FILE_ID}&confirm=true" --progress-bar \
		-o $@

DATA_DUMP = data/.unzipped
$(DATA_DUMP): data/dump.zip
	@touch $@
	mkdir -p data/raw
	unzip -oq data/dump.zip -d data/raw

ELECTION_SEASONS = data/raw/kansanedustajat_vaalikausittain.csv
$(ELECTION_SEASONS):
	mkdir -p data/raw
	FILE_ID=1bNQBZA6fxm3RYDSB7dT_D7fdYkEIUf_9
	curl -L "https://drive.usercontent.google.com/download?id=$${FILE_ID}&confirm=true" --progress-bar \
		-o $@

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
data: $(DATA_DUMP) $(MP_PHOTOS) $(ELECTION_SEASONS) ## download and extract all raw data assets

.PHONY: clean
clean: ## deletes all raw data assets
	rm -rf data/.[!.]*
	rm -f frontend/src/assets/.[!.]*


##################################
# Scripts for data preprocessing #
##################################

PIPE_DEPS = .venv/CACHEDIR.TAG
$(PIPE_DEPS): pyproject.toml uv.lock
	uv sync

.PHONY: install-pipes
install-pipes: $(PIPE_DEPS)

VASKI_DATA_DIR = data/raw/vaski
VASKI_DATA = $(VASKI_DATA_DIR)/.parsed
$(VASKI_DATA): pipes/vaski_parser.py $(DATA_DUMP)
	@echo "Parsing VASKI..."
	mkdir -p $(VASKI_DATA_DIR)
	uv run $<
	touch $@

.PHONY: clean-vaski
clean-vaski: ## removes vaski data
	rm -rf $(VASKI_DATA_DIR)

# Recipe for constructing all CSVs
$(PREPROCESSED)/%.csv: pipes/%_pipe.py $(DATA_DUMP) $(VASKI_DATA)
	@echo "Preprocessing $*..."
	mkdir -p $(PREPROCESSED)
	uv run $< --preprocess-data

# Prerequisites for preprocessing
$(PREPROCESSED)/election_seasons.csv: $(ELECTION_SEASONS)
$(PREPROCESSED)/government_proposals.csv: $(DB)/mps
$(PREPROCESSED)/mp_law_proposals.csv: $(DB)/mps
$(PREPROCESSED)/mps.csv: $(MP_PHOTOS)
$(PREPROCESSED)/mp_petition_proposals.csv: $(DB)/mps


.PHONY: preprocess
preprocess: $(addprefix $(PREPROCESSED)/,$(addsuffix .csv,$(PIPES)))

.PHONY: clean-preprocessed
clean-preprocessed: ## removes all preprocessed files
	rm -rf $(PREPROCESSED)


#################################
# Scripts for database creation #
#################################

# Recipe for inserting all data
$(DB)/%: data/preprocessed/%.csv
	@echo "Inserting $*..."
	mkdir -p $(DB)
	uv run pipes/$*_pipe.py --import-data
	touch $@

# Prerequisites for inserting data into database
$(DB)/agenda_items: $(DB)/sessions
$(DB)/committee_reports: $(DB)/mps $(DB)/committees
$(DB)/government_proposals: $(DB)/mps
$(DB)/interests: $(DB)/mps
$(DB)/ministers: $(DB)/mps
$(DB)/mp_committee_memberships: $(DB)/mps $(DB)/committees
$(DB)/mp_law_proposals: $(DB)/mps
$(DB)/mp_parliamentary_group_memberships: $(DB)/mps $(DB)/committees $(DB)/parliamentary_groups
$(DB)/speeches: $(DB)/mps
$(DB)/votes: $(DB)/ballots $(DB)/mps


.PHONY: database
database: $(addprefix $(DB)/,$(PIPES)) ## runs all data pipelines into the database

.PHONY: nuke
nuke: ## resets all data in the database
	PGPASSWORD=postgres psql -q -U postgres -h $${DATABASE_HOST:-db} postgres < DELETE_ALL_TABLES.sql
	PGPASSWORD=postgres psql -q -U postgres -h $${DATABASE_HOST:-db} postgres < postgres-init-scripts/01_create_tables.sql
	rm -rf $(DB)

.PHONY: nuke-database
nuke-database:
	$(MAKE) nuke
	$(MAKE) database

###############################
# Frontend management scripts #
###############################

FRONTEND_DEPS = frontend/node_modules/.package-lock.json
$(FRONTEND_DEPS): frontend/package.json frontend/package-lock.json
	cd frontend
	npm install

.PHONY: install-frontend
install-frontend: $(FRONTEND_DEPS)

.PHONY: frontend
frontend:
	cd frontend
	npm run dev

BUILD = frontend/dist/index.html
$(BUILD): install database $(shell find frontend/src -type f)
	cd frontend
	npm run build

.PHONY: build
build: $(BUILD)

.PHONY: deploy
deploy:
	cd frontend
	npm run deploy