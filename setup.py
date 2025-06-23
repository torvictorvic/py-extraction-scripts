try:
    from pip._internal.req import parse_requirements
except ImportError:
    from pip.req import parse_requirements
from setuptools import setup

requirements = [str(ir.requirement) for ir in parse_requirements('requirements.txt', session='hack')]

setup(
    name='xxxxxxxxxx_custom_scripts',
    description='Custom script for MercadoLibre Money In client',
    version='0.0.1',
    packages=["xxxxxxxxxx_custom_scripts"],
    include_package_data=True,
    url='',
    author='',
    author_email='',
    install_requires=requirements
)