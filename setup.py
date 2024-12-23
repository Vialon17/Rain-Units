# setup.py

from setuptools import setup, find_packages

setup(
    name = 'rainv',
    version = '0.1.0',
    packages = find_packages(exclude = [
        'rainv._inner_'
    ]),
    author = 'vialon',
    author_email = 'vialon109@gmail.com',
    description = 'Personal Tool Library',
    license = 'MIT',
    install_requires = [
        # List your dependencies here
        'pandas>=1.3.5',
        'requests>=2.31.0',
        'PyYAML>=6.0.1',
        'Flask>=2.2.5',
        'SQLAlchemy>=2.0.23',
        'qrcode>=7.4.2',
        'pillow>=10.3.0',
        'cryptography>=41.0.7',
        'openpyxl>=3.1.3',
    ],
)
