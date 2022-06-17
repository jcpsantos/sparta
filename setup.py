from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

long_description = (here / "README.md").read_text(encoding="utf-8")

required = ['azure-core==1.24.1', 'azure-storage-blob==12.12.0', 'boto3==1.24.7', 'botocore==1.27.7', 'chispa==0.9.2', 
            'pyspark==3.2.1', 'pytest==7.1.2', 'PyYAML==6.0','s3transfer==0.6.0', 'six==1.16.0', 'smart-open==6.0.0']
    
setup(
    name = 'sparta',
    version = '0.1.0',
    author = 'Juan Caio',
    author_email = 'juancaiops@gmail.com',
    packages = find_packages(),
    description = 'Library to help ETL using pyspark',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url = 'https://github.com/jcpsantos/sparta',
    install_requires = required,
    project_urls = {
        'Source code': 'https://github.com/jcpsantos/sparta'
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