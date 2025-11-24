from setuptools import setup, find_packages

setup(
    name="scripts",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        'pandas',
        'matplotlib',
    ],
    description="Helper functions for Yelp tourism data analysis"
)