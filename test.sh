#!/bin/sh
set -e
COURSE_DEFINITION=../../_others/redis-tester/internal/test_helpers/course_definition.yml
export CODECRAFTERS_SUBMISSION_DIR=$(pwd)
export CODECRAFTERS_TEST_CASES_JSON=$(yq -I0 '[.stages[] | {"slug": .slug, "tester_log_prefix": .slug, "title": (path | .[-1] + 1) + ") " + .name }]' -o=json $COURSE_DEFINITION)
../../_others/redis-tester/redis-tester.exe