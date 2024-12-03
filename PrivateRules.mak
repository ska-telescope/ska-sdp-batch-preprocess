ci-docs-build:
	make -C docs/ clean html SPHINXOPTS="--fail-on-warning --nitpicky --keep-going"

# Run all code checks necessary to pass the CI pipeline
ci: python-lint python-test ci-docs-build

.PHONY: ci-docs-build ci
