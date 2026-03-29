PYTHON ?= python3

.PHONY: bootstrap bootstrap-platform test verify-local run-batch run-streaming clean-local

bootstrap:
	bash scripts/bootstrap_local.sh

bootstrap-platform:
	WITH_PLATFORM=1 bash scripts/bootstrap_local.sh

test:
	$(PYTHON) -m pytest -q

verify-local:
	$(PYTHON) scripts/verify_local.py

run-batch:
	bash scripts/run_local_batch.sh

run-streaming:
	bash scripts/run_local_streaming.sh

clean-local:
	bash scripts/clean_local_state.sh
