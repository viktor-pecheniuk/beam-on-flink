import setuptools

setuptools.setup(
    name='portable-beam',
    version='0.1.0',
    install_requires=['xmltodict==0.12.0', 'python-snappy==0.6.0'],
    packages=setuptools.find_packages(),
)
