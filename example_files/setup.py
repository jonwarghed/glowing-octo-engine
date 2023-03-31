from setuptools import find_packages, setup

setup(
    name="example_files",
    packages=find_packages(exclude=["example_files_tests"]),
    install_requires=[
        "dagster",
        "requests",
        "boto3"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
