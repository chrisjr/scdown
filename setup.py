from codecs import open as codecs_open
from setuptools import setup, find_packages


# Get the long description from the relevant file
with codecs_open('README.rst', encoding='utf-8') as f:
    long_description = f.read()


setup(name='scdown',
      version='0.0.1',
      description=u"A collection of scripts for scraping SoundCloud.",
      long_description=long_description,
      classifiers=[],
      keywords='',
      author=u"Chris Johnson-Roberson",
      author_email='chris.johnson.roberson@gmail.com',
      url='https://github.com/chrisjr/scdown',
      license='MIT',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          'click',
          'celery',
          'boto',
          'py2neo',
          'pymongo',
          'flower
      ],
      extras_require={
          'test': ['pytest'],
      },
      entry_points="""
      [console_scripts]
      scdown=scdown.scripts.cli:cli
      """
      )
