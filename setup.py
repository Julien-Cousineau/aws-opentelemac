import setuptools
from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="awsopentelemac",
    version="1.0.0",
    author="Julien Cousineau",
    author_email="Julien.Cousineau@gmail.com",
    description="",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/meracan/awstools",
    packages=find_packages(),
    package_data={'': ['*.dico']},
    include_package_data=True,  
    install_requires=[],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)