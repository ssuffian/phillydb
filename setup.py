import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
NAME = "phillydb"
about = {}

with open(os.path.join(here, NAME, "__version__.py")) as f:
    exec(f.read(), about)

setup(
    name=NAME,
    version=about["__version__"],
    description=about["__description__"],
    entry_points={"console_scripts": [f"{NAME} = {NAME}:cli"]},
    packages=find_packages(),
    install_requires=["pandas", "requests"],
)
