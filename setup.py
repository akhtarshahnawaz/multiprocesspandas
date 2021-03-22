from setuptools import setup, find_packages

with open('README.md') as readme_file:
    README = readme_file.read()

with open('HISTORY.md') as history_file:
    HISTORY = history_file.read()

setup_args = dict(
    name='multiprocesspandas',
    version='1.1.1',
    description='Extends Pandas to run apply methods for  dataframe, series and groups on multiple cores at same time.',
    long_description_content_type="text/markdown",
    long_description=README + '\n\n' + HISTORY,
    license='MIT',
    packages=find_packages(),
    author='Shahnawaz Akhtar',
    author_email='shahnawaz.akhtar@barcelonagse.eu',
    keywords=['Pandas','Multiprocessing','Pandas Multiprocessing','Parallel','Parallize Pandas'],
    url='https://github.com/akhtarshahnawaz/multiprocesspandas',
    download_url='https://pypi.org/project/multiprocesspandas/'
)

install_requires = [
    'multiprocess',
    'pandas'
]

if __name__ == '__main__':
    setup(**setup_args, install_requires=install_requires)