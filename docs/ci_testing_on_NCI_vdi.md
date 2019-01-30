# Testing on NCI VDI platform

To test chages before pushing to [github](http://github.com)

1. Login to NCI VDI
1. Clone your repo to ``~/projects/fetch``
1. Create a virtual environment for testing
1. Perform Travis-style testing

## Virtual environment

Follow [virtualenvs howto](http://docs.python-guide.org/en/latest/dev/virtualenvs/) instructions

```
pip install virtualenv --user
virtualenv -p /apps/python/2.7.11/bin/python ./fetchenv/
source ~/fetchenv/bin/activate
python --version
cd projects/fetch
pip --version
pip install -r requirements.txt
python setup.py install
./check-code.sh
deactivate
```

After getting lots of crazy errors, I found I had to:

```
pip install --upgrade pylint astroid``
```
and then rerun ``./check-code.sh``

## Travis-style testing

You can perform CI testing before you commit your changes or push them to Github

```
cd ~/projects/fetch
source ~/fetchenv/bin/activate
python --version
cd projects/fetch
./check-code.sh
deactivate
```
