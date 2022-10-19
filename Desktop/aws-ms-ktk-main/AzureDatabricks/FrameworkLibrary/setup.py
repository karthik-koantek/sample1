import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="ktk",
    version="0.0.2",
    author="Eddie Edgeworth",
    author_email="edward.edgeworth@koantek.com",
    description="Koantek Framework Python Wheel",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
    install_requires=[
        'pyodbc',
        'jsonschema',
        'dataprep',
        'pandas_profiling',
        'missingno',
        'great_expectations'
    ]
)