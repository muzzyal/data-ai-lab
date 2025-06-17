# set to error out
set -e

echo Check for version file

VFILE=version.txt

if [ -f "$VFILE" ]; then
    echo "$VFILE exists"
else
    echo "$VFILE missing for image versioning"
    exit 1
fi

# create venv
python3 -m venv tmp
source tmp/bin/activate

# install test dependencies
poetry install

# run configuration tests:

# bandit checks for code misconfigurations excluding tests
bandit -x ./tests,./tmp -r .

# run unit tests
echo Running unit tests

if [ -f ".coveragerc" ]; then
    # If .coveragerc exists, run coverage run with the --rcfile option
    echo ".coveragerc found, running coverage with --rcfile"
    coverage run --omit="*test*" --rcfile=.coveragerc -m pytest -vv tests/
else
    # If .coveragerc doesn't exist, run coverage run without --rcfile
    echo ".coveragerc not found, running coverage without --rcfile"
    coverage run --omit="*test*" -m pytest -vv tests/
fi

echo Pytest finished with output code $?

coverage json

COVERAGE=$(jq '.totals.percent_covered_display | tonumber' coverage.json)

echo coverage finished with total coverage of $COVERAGE, full report below

# validate 70 % coverage of unit tests
coverage report

if [[ $COVERAGE -lt "70" ]] ; then
    echo Coverage must be at least 70%, please improve test coverage and try again
    exit 1
fi

#cleanup venv
deactivate
rm -r tmp
rm .coverage
rm coverage.json
