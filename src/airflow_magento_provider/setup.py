from setuptools import setup

setup(
    name='airflow-magento-provider',
    version='0.1',
    packages=['airflow_magento_provider'],
    install_requires=[
        'apache-airflow',
        'magento'
    ],
    extras_require={        
        'hooks': ['apache-airflow[hooks]'],
        'operators': ['apache-airflow[operators]'],
    },
)

