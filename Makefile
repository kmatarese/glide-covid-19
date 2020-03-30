ENV := $(HOME)/venvs/glide_covid
TEST_ENV := /tmp/glide_covid_19_pip_test/
PACKAGE_NAME := 'glide_covid_19'
VERSION := $(shell python setup.py --version)
EGG_OPTIONS := egg_info
PIP_CMD := $(ENV)/bin/pip
SETUP_CMD := $(ENV)/bin/python setup.py

all: install

clean:
	rm -rf build dist *.egg-info

develop:
	$(PIP_CMD) install -U -e ./ --no-binary ":all:"

install:
	$(SETUP_CMD) bdist_wheel $(EGG_OPTIONS)
	$(PIP_CMD) install -U dist/$(PACKAGE_NAME)-$(VERSION)-py3-none-any.whl

uninstall:
	if ($(PIP_CMD) freeze 2>&1 | grep $(PACKAGE_NAME)); \
		then $(PIP_CMD) uninstall $(PACKAGE_NAME) --yes; \
	else \
		echo 'No installed package found!'; \
	fi

dist:
	$(MAKE) clean
	$(SETUP_CMD) sdist bdist_wheel
