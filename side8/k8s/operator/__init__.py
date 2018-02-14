import kubernetes
from .patched_custom_objects_api import CustomObjectsApi as PatchedCustomObjectsApi
from .utils import parse
import sys
import subprocess
import os
import yaml
import json
import functools
import asyncio
import aiojobs
import logging
logging.getLogger(__name__).addHandler(logging.NullHandler())
logger = logging.getLogger()


async def handle_resource_change(apply_fn, delete_fn, api_update, api_delete, resource_object):
    namespace = resource_object['metadata']['namespace']
    name = resource_object['metadata']['name']
    uid = resource_object['metadata']['uid']
    logger_prefix = "{}/{} ({})".format(namespace, name, uid)
    resource_object['metadata'].setdefault('finalizers', [])
    patch_object = {}
    if resource_object['metadata'].get('deletionTimestamp', None) is not None:
        logger.info("{} marked for deletion".format(logger_prefix))
        if "Side8OperatorDelete" not in resource_object['metadata']['finalizers']:
            # We've already washed our hands of this one
            return
        logger.debug("{} calling delete_fn".format(logger_prefix))
        try:
            patch_object['status'] = await delete_fn(resource_object)
        except subprocess.CalledProcessError as e:
            # TODO write to k8s events here
            logger.warning("{} {} exited with {}".format(logger_prefix, e.cmd, e.returncode))
            return
        if not patch_object['status']:
            logger.debug("{} delete_fn returned empty status, removing finalizer".format(logger_prefix))
            patch_object['metadata'] = {
                    'finalizers': list(filter(lambda f: f != "Side8OperatorDelete", resource_object['metadata']['finalizers']))}
    else:
        logger.info("{} triggered by change".format(logger_prefix))
        if "Side8OperatorDelete" in resource_object['metadata']['finalizers']:
            logger.debug("{} calling apply_fn".format(logger_prefix))
            try:
                patch_object['status'] = await apply_fn(resource_object)
            except subprocess.CalledProcessError as e:
                # TODO write to k8s events here
                logger.warning("{} {} exited with {}".format(logger_prefix, e.cmd, e.returncode))
                return
        else:
            logger.debug("{} adding finalizer".format(logger_prefix))
            patch_object.setdefault('metadata', {})
            patch_object['metadata'].setdefault('finalizers', [])
            patch_object['metadata']['finalizers'].append("Side8OperatorDelete")

    logger.debug("{} calling api update".format(logger_prefix))
    api_update(patch_object)


async def resource_events_consumer(apply_fn, delete_fn, api_update, api_delete, queue, logger_prefix):
    while True:
        if not queue.qsize():
            logger.debug("{} falling out of empty consumer".format(logger_prefix))
            return
        # consume all but most recent change
        for i in range(1, queue.qsize()):
            queue.get_nowait()
            queue.task_done()
        resource_object = queue.get_nowait()

        namespace = resource_object['metadata']['namespace']
        name = resource_object['metadata']['name']
        uid = resource_object['metadata']['uid']

        logger_prefix = "{}/{} ({})".format(namespace, name, uid)
        logger.debug("{} event".format(logger_prefix))

        logger.debug("{} scheduling handler".format(logger_prefix))
        try:
            await handle_resource_change(apply_fn, delete_fn, api_update, api_delete, resource_object)
        finally:
            logger.info("{} has been handled".format(logger_prefix))
            queue.task_done()


async def events_consumer(custom_objects_api_instance, fqdn, version, resource, apply_fn, delete_fn, queue):
    scheduler = await aiojobs.create_scheduler(limit=10)

    resource_queues = {}
    while True:
        event = await queue.get()

        event_type = event['type']

        resource_object = event['object']
        namespace = resource_object['metadata']['namespace']
        name = resource_object['metadata']['name']
        uid = resource_object['metadata']['uid']
        logger_prefix = "{}/{} ({})".format(namespace, name, uid)
        logger.debug("{} {}".format(logger_prefix, event_type))

        if event_type in ["ADDED", "MODIFIED"]:
            if uid not in resource_queues:
                logger.debug("{} does not have a pre-existing consumer, spawning one".format(logger_prefix))
                api_update = functools.partial(custom_objects_api_instance.update_namespaced_custom_object,
                                               fqdn, version, namespace, resource, name)
                api_delete = functools.partial(custom_objects_api_instance.delete_namespaced_custom_object,
                                               fqdn, version, namespace, resource, name, body=kubernetes.client.V1DeleteOptions())
                resource_queue = asyncio.Queue()
                logger.debug("{} queueing".format(logger_prefix))
                resource_queue.put_nowait(resource_object)
                resource_queues[uid] = resource_queue

                # Closures, references, it's all so confusing.
                # Don't pass in the bits as default args like this and you'll have the del running on the wrong resources
                async def resource_events_consumer_wrapper(uid=uid, apply_fn=apply_fn, delete_fn=delete_fn, api_update=api_update, api_delete=api_delete, resource_queue=resource_queue, logger_prefix=logger_prefix):
                    try:
                        await resource_events_consumer(apply_fn, delete_fn, api_update, api_delete, resource_queue, logger_prefix)
                    finally:
                        logger.debug("{} has completed, removing GC preventing reference".format(logger_prefix))
                        del(resource_queues[uid])

                await scheduler.spawn(resource_events_consumer_wrapper())
            else:
                logger.debug("{} queueing".format(logger_prefix))
                resource_queues[uid].put_nowait(resource_object)

        queue.task_done()


async def generator_wrapper(sync_generator, _loop=None):
    _loop = asyncio.get_event_loop() if _loop is None else _loop
    while True:
        yield await _loop.run_in_executor(None, next, sync_generator)


async def api_events_sink(custom_objects_api_instance, fqdn, version, resource, queue):
    w = kubernetes.watch.Watch()
    event_generator = w.stream(custom_objects_api_instance.list_cluster_custom_object, fqdn, version, resource)
    async for event in generator_wrapper(event_generator):
        queue.put_nowait(event)


def main():

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--fqdn', required=True)
    parser.add_argument('--version', required=True)
    parser.add_argument('--resource', required=True)
    parser.add_argument('--apply', default="./apply")
    parser.add_argument('--delete', default="./delete")
    parser.add_argument('--log-level', default="info")

    args = parser.parse_args()

    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(module)s:%(funcName)s:%(lineno)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    log_level = getattr(logging, args.log_level.upper())
    logger.setLevel(log_level)

    try:
        kubernetes.config.load_incluster_config()
        logger.debug("configured in cluster with service account")
    except Exception:
        try:
            kubernetes.config.load_kube_config()
            logger.debug("configured via kubeconfig file")
        except Exception:
            logger.debug("No Kubernetes configuration found")
            sys.exit(1)

    custom_objects_api_instance = PatchedCustomObjectsApi()

    fqdn = args.fqdn
    version = args.version
    resource = args.resource

    async def callout_fn(callback, resource_object):
        namespace = resource_object['metadata']['namespace']
        name = resource_object['metadata']['name']
        uid = resource_object['metadata']['uid']
        logger_prefix = "{}/{} ({})".format(namespace, name, uid)
        logger.debug("{} running {}".format(logger_prefix, callback))
        subprocess_env = dict([("_DOLLAR", "$")] + parse(resource_object, prefix="K8S") + [("K8S", json.dumps(resource_object))])
        process = await asyncio.create_subprocess_exec(
            callback,
            env=dict(list(os.environ.items()) + list(subprocess_env.items())),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        out, err = await process.communicate()
        logger.debug("{} stdout: {}".format(logger_prefix, out))
        logger.debug("{} errout: {}".format(logger_prefix, err))
        if process.returncode != 0:
            logger.warn("{} errout: {}".format(logger_prefix, err))
            raise subprocess.CalledProcessError(process.returncode, callback)
        status = yaml.load(out)
        return status

    apply_fn = functools.partial(callout_fn, args.apply)
    delete_fn = functools.partial(callout_fn, args.delete)

    loop = asyncio.get_event_loop()

    if args.log_level == "DEBUG":
        loop.set_debug()

    queue = asyncio.Queue()

    events_consumer_coro = events_consumer(custom_objects_api_instance, fqdn, version, resource, apply_fn, delete_fn, queue)
    api_events_sink_coro = api_events_sink(custom_objects_api_instance, fqdn, version, resource, queue)

    loop.run_until_complete(asyncio.wait({api_events_sink_coro, events_consumer_coro}, return_when=asyncio.FIRST_COMPLETED))


if __name__ == '__main__':
    main()
