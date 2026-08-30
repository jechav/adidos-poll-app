# I-023: Locust load-test runner image.
#
# Deliberately a separate image from the app's own Dockerfile: the app
# image must never import `locust` (see loadtest/requirements.txt on
# why -- gevent's import-time monkey-patching), and the load generator
# must never run inside the same replica set as the service under test
# (see docs/issues/I-023-load-tests.md's Distributed Execution section:
# "deployed as Kubernetes Jobs alongside, not on, the service under
# test").

FROM python:3.12-slim

WORKDIR /loadtest

COPY loadtest/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY loadtest ./loadtest

ENTRYPOINT ["locust"]
