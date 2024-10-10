import os
from setuptools import setup, find_packages


here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.md'), encoding="utf-8") as f:
    long_description = f.read()

github_url = 'https://github.com/folknik/dqchecker'

setup(
    name="dqchecker",
    version="0.0.1",
    author="Alexey Fadeev",
    author_email="fadeev1087@gmail.com",
    license='MIT',
    description="Airflow package to check data quality",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url=github_url,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        'Programming Language :: Python :: 3.12',
    ],
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires='>=3.10, <4',
    install_requires=[
        'psycopg2-binary',
        'prometheus-client',
        'pyyaml'
    ],
)
