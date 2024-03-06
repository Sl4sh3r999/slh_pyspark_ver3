from setuptools import setup, find_packages

setup(
    name='slh_pyspark',
    version='3.0',
    packages=find_packages(),
    install_requires=[
        'pyspark',
        'apache-sedona'
    ],
    author='slasher Opositor',
    author_email='autoami99@hotmail.com',
    description='Funciones que te ayudan con cosas que deberían ser simples en pyspark',
    url='https://github.com/Sl4sh3r999/slh_pyspark_ver3'
)
