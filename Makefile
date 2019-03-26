
#
# Default.
#

default: build

#
# Tasks.
#

# Build.
build: 
	@pip3 install .

# Dev.
dev:
	@pip3 install -e .

# Deploy.
release:
	@python3 setup.py sdist upload

# Test.
test:
	@pylint tap_shopify -d missing-docstring

#
# Phonies.
#

.PHONY: build
.PHONY: dev
.PHONY: release
.PHONY: schema
.PHONY: test

