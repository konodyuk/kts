import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open('VERSION', 'r') as f:
    VERSION = int(f.read().strip())

VERSION += 1

with open('VERSION', 'w') as f:
    f.write(str(VERSION))

setuptools.setup(
    name="kts",
    version=f"0.2.{VERSION}",
    author="Nikita Konodyuk",
    author_email="konodyuk@gmail.com",
    description="Competition-oriented framework for interactive feature engineering and building reproducible pipelines",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/konodyuk/kts",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "mprop",
        "pandas",
        "numpy",
        "scikit-learn",
        "scikit-optimize",
        "matplotlib",
        "dill",
        "feather-format",
        "xgboost",
        "lightgbm",
        "catboost",
        "swifter",
        "kts-cli",
        "texttable"
    ]
)
