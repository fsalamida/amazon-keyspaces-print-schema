from setuptools import setup
setup(
    name='print-schema',
    version='1.0',
    include_package_data=True,
    packages=['src'],
    entry_points = {
        'console_scripts': ['amazon-keyspaces-print-schema=src.main:main'],
    },

    install_requires=[
        'cassandra-sigv4',
        'pandas',
        'openpyxl'
    ]
)

