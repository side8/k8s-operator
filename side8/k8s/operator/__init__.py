import kubernetes
from .patched_custom_objects_api import CustomObjectsApi as PatchedCustomObjectsApi
from .utils import parse
import sys
import subprocess
import os
import yaml
from contextlib import suppress
import urllib3.exceptions
import json
import functools
import asyncio


def handle_resource_change(custom_objects_api_instance, apply_fn, delete_fn, api_update, api_delete, resource_object):
    # print("handling {}'s {}".format(resource_object['metadata']['namespace'], resource_object['metadata']['name']))
    # print("{} {}".format(resource_object['metadata'].get('finalizers', []), resource_object['metadata'].get('deletionTimestamp',None)))
    resource_object['metadata'].setdefault('finalizers', [])
    patch_object = {}
    if resource_object['metadata'].get('deletionTimestamp', None) is not None:
        if "Side8OperatorDelete" in resource_object['metadata']['finalizers']:
            patch_object['status'] = delete_fn(resource_object)
            if not patch_object['status']:
                patch_object['metadata'] = {
                        'finalizers': list(filter(lambda f: f != "Side8OperatorDelete", resource_object['metadata']['finalizers']))}
    else:
        if "Side8OperatorDelete" in resource_object['metadata']['finalizers']:
            try:
                patch_object['status'] = apply_fn(resource_object)
            except subprocess.CalledProcessError as e:
                # TODO log k8s error event
                print("{} exited with {}".format(e.cmd, e.returncode))
                return
        else:
            patch_object.setdefault('metadata', {})
            patch_object['metadata'].setdefault('finalizers', [])
            patch_object['metadata']['finalizers'].append("Side8OperatorDelete")

    api_update(patch_object)


async def wait_events(loop, custom_objects_api_instance, fqdn, version, resource, apply_fn, delete_fn):
    w = kubernetes.watch.Watch()
    resource_events = {}
    while True:
        event_generator = w.stream(custom_objects_api_instance.list_cluster_custom_object, fqdn, version, resource)
        done, pending = set(), set()
        event_coro = None
        handler_coro = None

        with suppress(urllib3.exceptions.ReadTimeoutError):
            while True:

                if event_coro in done:
                    print("====> event coro in done")
                    event = await event_coro
                    event_type = event['type']
                    if event_type in ["ADDED", "MODIFIED"]:
                        object = event['object']
                        object_uid = object['metadata']['uid']
                        resource_events[object_uid] = object
                    if event_type == "DELETED":
                        del(resource_events[object_uid])

                if event_coro not in pending:
                    print("====> event coro not in pending")
                    event_coro = loop.run_in_executor(None, lambda: next(event_generator))
                    pending.add(event_coro)

                if handler_coro not in pending:
                    try:
                        _uid, resource_object = resource_events.popitem()
                    except KeyError:
                        pass
                    else:
                        namespace = object['metadata']['namespace']
                        name = object['metadata']['name']
                        api_update = functools.partial(custom_objects_api_instance.update_namespaced_custom_object,
                                                       fqdn, version, namespace, resource, name)
                        api_delete = functools.partial(custom_objects_api_instance.delete_namespaced_custom_object,
                                                       fqdn, version, namespace, resource, name, body=kubernetes.client.V1DeleteOptions())

                        handler_coro = loop.run_in_executor(None, handle_resource_change, custom_objects_api_instance, apply_fn, delete_fn,
                                                            api_update, api_delete, resource_object)
                        pending.add(handler_coro)

                done, pending = await asyncio.wait(pending)


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
        print("running {} for {}'s {}".format(callback, event_object['metadata']['namespace'], event_object['metadata']['name']))
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

    loop = asyncio.get_event_loop()
    wait_events_coro = wait_events(loop, custom_objects_api_instance, fqdn, version, resource, apply_fn, delete_fn)
    loop.run_until_complete(wait_events_coro)


if __name__ == '__main__':
    main()
