.PHONY: build

VENV=${HOME}/.venv38/bin

build:
	$(VENV)/python -m build

