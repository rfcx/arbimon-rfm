# The binary to build (just the basename).
MODULE := rfm
SCRIPT ?= cli

# Where to push the docker image.
REGISTRY ?= 887044485231.dkr.ecr.eu-west-1.amazonaws.com

IMAGE := $(REGISTRY)/$(MODULE)

# This version-strategy uses git tags to set the version string
TAG := $(shell git describe --tags --always --dirty)

BLUE='\033[0;34m'
NC='\033[0m' # No Color

run:
	@python -m $(MODULE) $(ARGS)

test:
	@pytest

lint:
	@echo "\n${BLUE}Running Flake8 against source and test files...${NC}\n"
	@flake8
	@echo "\n${BLUE}Running Bandit against source files...${NC}\n"
	@bandit -r -c bandit.yaml $(MODULE)

lint-fix:
	@echo "\n${BLUE}Running autoflake8 against source and test files...${NC}\n"
	@autoflake8 --in-place -r rfm tests

build-dev:
	@echo "\n${BLUE}Building development image with labels:\n"
	@echo "name: $(MODULE)"
	@echo "version: $(TAG)${NC}\n"
	@docker build -t $(MODULE):$(TAG) -f build/Dockerfile .

# Example: make shell CMD="-c 'date > datefile'"
shell: build
	@echo "\n${BLUE}Launching a shell in the containerized build environment...${NC}\n"
	@docker run                                                 \
		-ti                                                     \
		--rm                                                    \
		--entrypoint /bin/bash                                  \
		-v ${PWD}:/app                                          \
		-u $$(id -u):$$(id -g)                                  \
		$(MODULE):$(TAG)										\
		$(CMD)

serve-up:
	@echo "\n${BLUE}Running docker compose up...${NC}\n"
	@docker compose up -d --wait --build
	@echo "\n${BLUE}Seeding s3mock... (might take a few minutes)${NC}\n"
	@sleep 3
	@docker compose run --rm -v ${PWD}/store/mock-data/core-bucket:/up -v ${PWD}/store/upload.sh:/upload.sh -e UPLOAD_FOLDER=/up app bash /upload.sh
	@echo "\n${BLUE}Dev environment ready!"
	@echo "    use \`make serve-run SCRIPT=batch_legacy\` to run"
	@echo "    use \`make serve-down\` when you are finished${NC}\n"

serve-run:
	@echo "\n${BLUE}Launching $(SCRIPT) in docker...${NC}\n"
	@docker compose exec app python -m $(MODULE).$(SCRIPT)

serve-down:
	@echo "\n${BLUE}Running docker compose down...${NC}\n"
	@docker compose down -v

version:
	@echo $(TAG)

clean:
	rm -rf .pytest_cache .coverage .pytest_cache coverage.xml

docker-clean:
	@docker system prune -f --filter "label=name=$(MODULE)"