from setuptools import setup, find_packages

setup(
    name='apache-airflow-provider-magento',
    version='0.4',
    description="custom provider for Magento 2 - Apache Airflow",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author="Sivakumar Koduru",
    author_email="sivajik34@gmail.com",
    url="https://github.com/sivajik34/magento-airflow",    
    install_requires=[
        "apache-airflow>=2.8.0",
        "requests_oauthlib",
        "requests"
    ],
    packages=find_packages(),
    python_requires='>=3.8'  
    
)

