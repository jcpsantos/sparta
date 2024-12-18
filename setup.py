from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

long_description = (here / "README.md").read_text(encoding="utf-8")

required = ["azure-storage-blob>=12.12.0", "boto3>=1.20.24", "chispa>=0.9.2", "pyspark>=3.2.1", "pytest>=3.2.2", "PyYAML>=6.0", "smart-open>=6.0.0", "delta-spark>=3.2.1"]

dev_packages = [
    "pytest>=7.0.0",
    "pytest-cov>=3.0.0",
    "mypy>=0.942",
    "types-pyyaml>=6.0.7",
    "flake8>=4.0.1",
    "docstr-coverage>=2.2.0",
]
    
setup(
    name = 'pysparta',
    version = '0.5.4',
    author = 'Juan Caio',
    author_email = 'juancaiops@gmail.com',
    packages = find_packages(),
    description = 'Library to help ETL using pyspark',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url = 'https://github.com/jcpsantos/sparta',
    install_requires = required,
    extras_require={"dev": dev_packages},
    python_requires=">= 3.7",
    project_urls = {
        'Source code': 'https://github.com/jcpsantos/sparta',
        'Documentation': 'https://jcpsantos.github.io/sparta/'
    },
    license = 'GNU General Public License v2.0',
    keywords = 'spark etl data sparta',
    classifiers = [
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Internationalization',
    ]
)