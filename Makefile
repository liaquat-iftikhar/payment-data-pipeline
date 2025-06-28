# Makefile for Python data engineering repo

.PHONY: help setup install test lint format clean

help:
	@echo "Usage:"
	@echo "  make setup       # Set up virtual environment and install dependencies"
	@echo "  make install     # Install dependencies into the current environment"
	@echo "  make test        # Run all tests"
	@echo "  make lint        # Run code linting (flake8)"
	@echo "  make format      # Format code (black)"
	@echo "  make clean       # Remove __pycache__, .pytest_cache, and .pyc files"

setup:
	python3 -m venv venv
	. venv/bin/activate && pip install --upgrade pip setuptools wheel
	. venv/bin/activate && pip install -r requirements.txt

install:
	pip install -r requirements.txt

test:
	pytest tests/

lint:
	flake8 pipeline tests

format:
	black pipeline tests

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache
	rm -rf venv

