#!/usr/bin/env python

from contextlib import contextmanager
import os

from setuptools import find_packages, setup


def find_deploy_scripts(path, include_patterns, exclude_patterns=[]):
    cmd = "FILES=`find %s -path %s" % (path, (" -o -path ").join(include_patterns))
    if exclude_patterns:
        cmd += " | grep -v -E '(%s)'" % ("|").join(exclude_patterns)
    cmd += "`;"
    cmd += " for FILE in $FILES; do if [ `echo $FILE | xargs grep -l '/usr/bin/env python'` ] || [ `echo $FILE | grep -v .py` ]; then echo $FILE; fi; done"
    h = os.popen(cmd)
    out = h.read()
    h.close()
    return out.split()


@contextmanager
def load_file(fname):
    f = open(os.path.join(os.path.dirname(__file__), fname))
    try:
        yield f
    finally:
        f.close()


with load_file("README.md") as f:
    README = f.read()

with load_file("requirements.txt") as f:
    requires = f.read().split("\n")

# Split git requirements to fill in dependency_links
git_requires = [x for x in requires if "git" in x]
non_git_requires = [x for x in requires if "git" not in x]
for repo in git_requires:
    # Append git egg references
    non_git_requires.append(repo.split("egg=")[-1])

extras_require = {}
extras_require["complete"] = sorted(set(sum(extras_require.values(), [])))

exec(open("glide_covid_19/version.py").read())

setup(
    name="glide_covid_19",
    description="Use Glide to pull COVID-19 data",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/kmatarese/glide_covid_19",
    author="Kurt Matarese",
    author_email="none@none.com",
    maintainer="Kurt Matarese",
    version=__version__,
    license="MIT",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
    ],
    python_requires=">=3.6",
    scripts=find_deploy_scripts("glide_covid_19", ["\\*.py", "\\*.sh", "\\*.sql"], ["__init__"]),
    packages=find_packages(exclude=["tests.*", "tests"]),
    include_package_data=True,
    install_requires=non_git_requires,
    dependency_links=git_requires,
    extras_require=extras_require,
)
