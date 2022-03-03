# This is an example Makefile. It provides all of the "basic" operations that
# developers would expect from your repository -- build, clean, test -- as well
# as a few more operations that are specific to packaging up and testing Docker
# containers.
#
# You should feel free to modify this for your project... just take it as an
# example!

# Standard settings that will be used later
ROOT_DIR      := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
VENV_CMD      := python3 -m venv
VENV_DIR      := $(ROOT_DIR)/.venv
VENV_BIN      := $(VENV_DIR)/bin
AWS_BIN       := $(VENV_BIN)/aws
KINGPIN_BIN   := $(VENV_BIN)/kingpin

# Bring in the Docker.mk..
include Docker.mk

SHA1 := $(shell git rev-parse --short HEAD)

# The repository name should be easy enough to get - We append the .git so that
# we are matching exactly how it looks in Github. Note - 
_REPO_NAME         := $(shell basename `git config --get remote.origin.url`)
REPO_NAME          := $(shell basename -s .git $(_REPO_NAME))

# see go/ecr
ECR_REGION         := us-west-2
ECR_ACCOUNT_ID     := 364942603424
ECR_REGISTRY       := $(ECR_ACCOUNT_ID).dkr.ecr.$(ECR_REGION).amazonaws.com
# Use 'nextdoor' if this will be a production service
ECR_NAMESPACE      := dev

# Are we DRY? Automatically default us to DRY.
DRY ?= true
ifeq ($(DRY),false)
  KINGPIN := $(KINGPIN_BIN) --color
else
  KINGPIN := $(KINGPIN_BIN) --color --dry
endif

###############################################################################
# Development Environment Setup
###############################################################################
  
venv: $(VENV_DIR)
  
$(VENV_BIN)/activate:
	$(VENV_CMD) $(VENV_DIR)
  
$(AWS_BIN): $(VENV_BIN)/activate
	$(VENV_BIN)/pip install awscli
  
$(VENV_DIR): $(VENV_BIN)/activate requirements.txt
	$(VENV_BIN)/pip install -r requirements.txt && touch $(VENV_DIR)

git_submodules:
	git submodule sync && git submodule update --init

docker_run: docker_stop
	@echo "Running $(DOCKER_IMAGE)"
	@$(DOCKER) run \
		--name "$(DOCKER_IMAGE)" \
		--detach \
		--publish 8443:443 \
		"$(DOCKER_IMAGE)"

docker_test:
	$(DOCKER) exec $(DOCKER_IMAGE) curl --fail --insecure https://localhost/healthz

docker_stop:
	@echo "Stopping $(DOCKER_IMAGE)"
	@$(DOCKER) stop "$(DOCKER_IMAGE)" && $(DOCKER) rm "$(DOCKER_IMAGE)" \
		|| echo "No existing container running."

deploy:
	DOCKER_TAG=$(DOCKER_TAG) $(KINGPIN) --color --script deploy.yaml
