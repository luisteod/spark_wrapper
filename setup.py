from setuptools import setup, find_packages

setup(
    name='spark',
    version='1.0',
    packages=find_packages(),
    install_requires=[
        'pyspark',
    ],
    include_package_data=True,
    author='Luis Henrique'
)