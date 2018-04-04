ARG BASE=jessie
# ARG BASE=3.6-alpine3.7


FROM python:3.6-${BASE}

ARG BASE=jessie

RUN echo "${BASE}" | grep -Eq 'alpine' && apk add --no-cache alpine-sdk || true

RUN pip install --no-cache-dir pyinstaller
# RUN apt-get update && apt-get install -y gettext-base
ADD . /k8s-operator/
WORKDIR k8s-operator
RUN pyinstaller -F -n side8-k8s-operator side8/k8s/operator/__init__.py


FROM scratch

COPY --from=0 k8s-operator/dist/side8-k8s-operator /
