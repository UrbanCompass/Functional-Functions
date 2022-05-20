import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="functional-functions",
    version = "0.6.1",
    author = "Lawrence Chin",
    author_email = "lawrence.chin@compass.com",
    description = "Commonly used functions by the Compass FBI Team",
    long_description = long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/UrbanCompass/Functional-Functions",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent", 
    ],
    python_requires='>=3.6',
    install_requires=[
        "snowflake-connector-python >= 2.1.1",
        "snowflake-sqlalchemy >= 1.2.4",
        "pandas >= 1.1.4",
        "numpy >= 1.19.4",
        "pytz >= 2020.4",
        "pyarrow >= 5.0.0, <= 6.0.0",
        "boto3 >= 1.18.54",
        "redshift-connector >= 2.0.902",
        "databricks-sql-connector >= 2.0.0b1",
        "python-dotenv>=0.19.0"
    ],
    include_package_data=True,
    package_data={'': ['settings.py.sample','ff_classes.py','creds.env.sample']}
)