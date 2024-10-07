install:
	pip install --upgrade pip && pip install -r requirements.txt

format:
	black *.py

lint:
	ruff check *.py my_lib/*.py

test:
	python -m pytest -cov=main test_main.py

all: install format lint test