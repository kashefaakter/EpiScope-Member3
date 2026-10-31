
# EpiScope - Project Makefile

PYTHON = python

.PHONY: help install test check clean

help:
	@echo "EpiScope Project Commands:"
	@echo "  make install  - Install project dependencies"
	@echo "  make test     - Run project tests"
	@echo "  make check    - Check Python files for syntax errors"
	@echo "  make clean    - Remove Python cache files"

install:
	$(PYTHON) -m pip install -r requirements.txt

test:
	$(PYTHON) -m pytest tests/

check:
	$(PYTHON) -m compileall -q pipeline analytics orchestration

clean:
	$(PYTHON) -c "import pathlib, shutil; [shutil.rmtree(p, ignore_errors=True) for p in pathlib.Path('.').rglob('__pycache__')]; [p.unlink() for p in pathlib.Path('.').rglob('*.pyc')]"
