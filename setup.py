from setuptools import setup, find_packages

setup(
    name="aegis-dist",
    version="1.0.0",
    description="A ground-up, zero-dependency, fault-tolerant distributed consensus, storage, and transaction engine.",
    author="Aegis Core Distributed Systems Team",
    author_email="dev@aegis.systems",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[],
    entry_points={
        "console_scripts": [
            "aegis=main:main",
        ],
    },
)
