import kubernetes
from .patched_custom_objects_api import CustomObjectsApi as PatchedCustomObjectsApi
from .utils import parse
import sys
import subprocess
import os
import yaml
from contextlib import suppress
import urllib3.exceptions
import socket
import json
import functools


def wait_events(custom_objects_api_instance, fqdn, version, resource, apply_fn, delete_fn):
    w = kubernetes.watch.Watch()
    while True:
        with suppress(urllib3.exceptions.ReadTimeoutError, socket.timeout):
            for event in w.stream(custom_objects_api_instance.list_cluster_custom_object, fqdn, version, resource, _request_timeout=60):
                patch_object = {}
                object = event['object']
                namespace = object['metadata']['namespace']
                name = object['metadata']['name']
                object['metadata'].setdefault('finalizers', [])
                event_type = event['type']
                if event_type in ["ADDED", "MODIFIED"]:
                    if object['metadata'].get('deletionTimestamp', None) is not None:
                        if "Side8OperatorDelete" in object['metadata']['finalizers']:
                            patch_object['status'] = delete_fn(event['object'])
                            if not patch_object['status']:
                                patch_object['metadata'] = {'finalizers': list(filter(lambda f: f != "Side8OperatorDelete", object['metadata']['finalizers']))}
                        else:
                            try:
                                custom_objects_api_instance.delete_namespaced_custom_object(
                                        fqdn, version, namespace, resource,
                                        name, body=kubernetes.client.V1DeleteOptions())
                            except kubernetes.client.rest.ApiException as e:
                                if e.status != 404:
                                    # We're likely seeing an event for a resource that's already been deleted, ignore
                                    raise
                            except subprocess.CalledProcessError as e:
                                # TODO log k8s error event
                                print("{} exited with {}".format(e.cmd, e.returncode))
                                continue
                            continue
                    else:
                        if "Side8OperatorDelete" in object['metadata']['finalizers']:
                            try:
                                patch_object['status'] = apply_fn(event['object'])
                            except subprocess.CalledProcessError as e:
                                # TODO log k8s error event
                                print("{} exited with {}".format(e.cmd, e.returncode))
                                continue
                        else:
                            patch_object.setdefault('metadata', {})
                            patch_object['metadata'].setdefault('finalizers', [])
                            patch_object['metadata']['finalizers'].append("Side8OperatorDelete")

                    custom_objects_api_instance.update_namespaced_custom_object(
                            fqdn, version, namespace, resource, name,
                            patch_object)


def main():

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--fqdn', required=True)
    parser.add_argument('--version', required=True)
    parser.add_argument('--resource', required=True)
    parser.add_argument('--apply', default="./apply")
    parser.add_argument('--delete', default="./delete")

    args = parser.parse_args()

    try:
        kubernetes.config.load_incluster_config()
        print("configured in cluster with service account")
    except Exception:
        try:
            kubernetes.config.load_kube_config()
            print("configured via kubeconfig file")
        except Exception:
            print("No Kubernetes configuration found")
            sys.exit(1)

    custom_objects_api_instance = PatchedCustomObjectsApi()

    fqdn = args.fqdn
    version = args.version
    resource = args.resource

    def callout_fn(callback, event_object):
        print("running {}".format(callback))
        subprocess_env = dict([("_DOLLAR", "$")] + parse(event_object, prefix="K8S") + [("K8S", json.dumps(event_object))])
        process = subprocess.Popen(
            [callback],
            env=dict(list(os.environ.items()) + list(subprocess_env.items())),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=1)
        out, err = process.communicate()
        print("out: {}".format(out))
        print("error:")
        print(err.decode('utf-8'))
        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, callback)
        status = yaml.load(out)
        return status

    apply_fn = functools.partial(callout_fn, args.apply)
    delete_fn = functools.partial(callout_fn, args.delete)

    wait_events(custom_objects_api_instance, fqdn, version, resource, apply_fn, delete_fn)


if __name__ == '__main__':
    main()
