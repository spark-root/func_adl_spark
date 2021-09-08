==============
func_adl_spark
==============


.. image:: https://img.shields.io/pypi/v/func_adl_spark.svg
        :target: https://pypi.python.org/pypi/func_adl_spark

.. image:: https://img.shields.io/travis/PerilousApricot/func_adl_spark.svg
        :target: https://travis-ci.com/PerilousApricot/func_adl_spark

.. image:: https://readthedocs.org/projects/func-adl-spark/badge/?version=latest
        :target: https://func-adl-spark.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status




Spark backend for func_adl


* Free software: MIT license
* Documentation: https://func-adl-spark.readthedocs.io.


Features
--------

* TODO

Development Environment
-----------------------
To checkout the development environment, run the following:

```
git clone git@github.com:spark-root/func_adl_spark.git
cd func_adl_spark
python3.7 -m venv venv
source venv/bin/activate
pip install --upgrade pip wheel setuptools
pip install --prefer-binary -r requirements_dev.txt
pip install -e .
```

 Due to the dependency on Spark 2.4, we currently require Python3.4->Python3.7.
 MacOS has moved to 3.9 in a recent version, so to get/use a different version
 with Homebrew, you can instead execute the following (modify if necessary if
 you have another method to access Python3.7):
```
brew install python@3.7
git clone git@github.com:spark-root/func_adl_spark.git
cd func_adl_spark
/usr/local/opt/python@3.7/bin/python3.7 -m venv venv
source venv/bin/activate
pip install --upgrade pip wheel setuptools
pip install --prefer-binary -r requirements_dev.txt
pip install -e .
```

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
