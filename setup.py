from setuptools import setup, find_packages

with open("README.md") as readme_file:
    readme = readme_file.read()

with open("HISTORY.md") as history_file:
    history = history_file.read()

requirements = [
    "pyspark",
]

setup_requirements = []

test_requirements = [
    "black",
    "pytest",
]

setup(
    author="n.yokota",
    author_email="n.yokota@kakehashi.life",
    classifiers=[
        "Natural Language :: Japanese",
        "Programming Language :: Python :: 3.8",
    ],
    description="handle pyspark schema",
    install_requires=requirements,
    long_description=readme + "\n\n" + history,
    include_package_data=True,
    keywords="pysparkschema",
    name="pysparkschema",
    packages=find_packages(),
    setup_requires=setup_requirements,
    test_suite="tests",
    extras_require={"test": test_requirements},
    url="https://github.com/n-yokota/pysparkschema",
    version="0.1.0",
    zip_safe=False,
)

