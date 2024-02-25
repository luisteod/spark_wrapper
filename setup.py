from setuptools import setup, find_packages

setup(
    name='spark',
    version='1.0',
    packages=find_packages(),
    install_requires=[
        'pyspark',
    ],
    author='Luis Henrique'
)