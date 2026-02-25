PYTHONPATH=src

.PHONY: test regenerate-golden

test:
	PYTHONPATH=src pytest -q

regenerate-golden:
	PYTHONPATH=src python scripts/regenerate_golden.py
