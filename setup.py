from setuptools import setup, find_packages
from pathlib import Path

# Read README.md for long description
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name="fpt-api",
    version="0.3.0",
    packages=find_packages(where="python"),
    package_dir={"": "python"},
    install_requires=[
        "shotgun_api3",
        "urllib3",
        "certifi",
    ],
    python_requires=">=3.7",
    long_description=long_description,
    long_description_content_type="text/markdown",
    description="A thread-safe wrapper around Flow Production Tracking (formerly ShotGrid) with query field support",
    author="Kevin Sallee",
    author_email="kevin.sallee@gmail.com",
    url="https://github.com/ksallee/fpt-api",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)