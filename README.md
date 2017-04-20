# Install

Clone repository, install packages from *requirements.txt*.
```
git clone git@github.com:mashaletova/cday.git
cd cday
python3 -m venv cday
. bin/activate
pip3 install -r requirements.txt
```

# Usage

**main.py** script continuously polls API and writes new values to the database. You can specify sleep time between subsequent
API queries by specifying `--timeout [seconds]` option. You can also reset database before writing to it using the `--reset` flag.

**report.py** script queries the database, using pre-defined SQL queries. You can view available report options via help menu:
`python3 report.py --help`.

Tested with Python 3.4
