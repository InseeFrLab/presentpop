#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages
from setuptools.command.develop import develop
from setuptools.command.install import install
from subprocess import check_call

pgdriver_installation = """echo "Acquire::http::Proxy \"http://proxy-rie.http.insee.fr:8080\";" | sudo tee -a /etc/apt/apt.conf.d/proxy.conf \
&& sudo apt -y install libpq-dev && sudo export http_proxy=http://proxy-rie.http.insee.fr:8080"""


class PreDevelopCommand(develop):
    """Pre-installation for development mode."""

    def run(self):
        check_call(pgdriver_installation.split())
        develop.run(self)


class PreInstallCommand(install):
    """Pre-installation for installation mode."""

    def run(self):
        check_call(pgdriver_installation.split())
        print("pre-installed complete")
        install.run(self)


with open("README.rst") as readme_file:
    readme = readme_file.read()

setup_requirements = ["pyyaml"]

with open("requirements.txt", "r") as requirements_file:
    requirements = requirements_file.read().split("\n")

test_requirements = ["pytest>=3", "flake8"]

setup(
    author="Haixuan Xavier Tao",
    author_email="hai-xuan.tao@insee.fr",
    python_requires=">=3.5",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    description="Utils function necessary for project related to mobitic \
    such as importing data, common transformation function, etc...",
    entry_points={
        "console_scripts": [
            "mobitic_utils=mobitic_utils.__main__:main",
        ],
    },
    install_requires=requirements,
    license="MIT license",
    long_description=readme,
    include_package_data=True,
    keywords="mobitic_utils",
    name="mobitic_utils",
    packages=find_packages(include=["mobitic_utils", "mobitic_utils.*"]),
    setup_requires=setup_requirements,
    package_data={"mobitic_utils": ["mobitic_utils/*.yml"]},
    test_suite="tests",
    tests_require=test_requirements,
    cmdclass={
        "develop": PreDevelopCommand,
        "install": PreInstallCommand,
    },
    url="https://github.com/haixuantao/mobitic_utils",
    version="0.1.0",
    zip_safe=False,
)
