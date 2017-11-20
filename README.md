# k8s-operator

Write simple Kubernetes operators in a few lines of bash (or your favourite language).

Currently you should only run one copy per controller as there is no locking and the behaviour of 2 is undefined.

## Getting started

Create your CRD
```
$ cat es-crd.yaml
apiVersion: apiextensions.k8s.io/v1beta1
kind: CustomResourceDefinition
metadata:
  name: elasticsearchs.db.side8.io
Description: ElasticSearch
spec:
  group: db.side8.io
  version: v1
  scope: Namespaced
  names:
    kind: ElasticSearch
    singular: elasticsearch
    plural: elasticsearchs
    shortNames:
      - es
$ kubectl apply -f es-crd.yaml
```

create your apply and delete scripts:
```
echo "env | grep K8S >&2" > apply ; chmod +x apply
echo "env | grep K8S >&2" > delete ; chmod +x delete
```

run your operator in a dedicated terminal:
```
side8-k8s-operator --fqdn=db.side8.io --version=v1 --resource=elasticsearchs
```

create a custom resource and see the environment variables made available to your script:
```
$ cat es-test-cluster.yaml
apiVersion: "db.side8.io/v1"
kind: ElasticSearch
metadata:
  name: es-test-cluster
spec:
  master:
    replicas: 1
  data:
    replicas: 2
$ kubectl apply -f es-test-cluster.yaml
```

## apply

`apply` receives the configuration for custom resources in the form of environment variables. the above Elasticsearch resource will result in `K8S_METADATA_NAME=es-test-cluster`, `K8S_SPEC_MASTER_REPLICAS=1`, etc being available in the environment variables.

`apply` *must* only write valid yaml to stdout.

Any yaml written to stdout is captured and saved on the status field of the custom resource. This means on the next iteration it can be accessed in the environment prefixed with `K8S_STATUS_` like any other field in the custom resoruce.

This pattern allows very simple scripts to take deterministic single steps towards their ideal state.

## delete

`delete` receives the configuration environment variables in the same way as apply.

`delete` *must* only write either valid yaml _or_ nothing (techincally yaml null) to stdout.

If it writes yaml this will be written to the `status` field of the custom resource without deleting the resource and delete will be called again.
This is to allow convergant deletes without requiring complex logic in the delete command.

If it writes nothing the object is permanantly deleted from Kubernetes.

## Examples

https://github.com/side8/k8s-elasticsearch-operator is an Elasticsearch operator that safely manages Elasticsearch lifecycle events.
