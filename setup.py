from pathlib import Path
import setuptools

requirements = Path(__file__).parent / "requirements.txt"


with requirements.open("r") as fh:
    install_requires = fh.readlines()
    setuptools.setup(install_requires=install_requires)
