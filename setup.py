from setuptools import find_packages, setup

setup(
    name="ghg-quant",
    version="0.1.0",
    packages=find_packages(include=['src', 'src.*']),
    install_requires=[
        "pandas>=2.0.0",
        "numpy>=1.24.0",
    ],
)
