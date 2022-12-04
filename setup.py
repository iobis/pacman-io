from setuptools import setup, find_packages

setup(name="pacmanio",
      version="0.1.0",
      python_requires=">=3.6",
      url="https://github.com/iobis/pacman-io",
      license="MIT",
      author="Pieter Provoost",
      author_email="p.provoost@unesco.org",
      description="PacMAN data transformations",
      packages=find_packages(),
      zip_safe=False)
