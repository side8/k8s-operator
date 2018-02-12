import kubernetes.client
from six import iteritems


class CustomObjectsApi(kubernetes.client.CustomObjectsApi):
    def update_namespaced_custom_object(self, group, version, namespace, plural, name, body, **kwargs):
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async'):
            return self.update_namespaced_custom_object_with_http_info(group, version, namespace, plural, name, body, **kwargs)
        else:
            (data) = self.update_namespaced_custom_object_with_http_info(group, version, namespace, plural, name, body, **kwargs)
            return data

    def update_namespaced_custom_object_with_http_info(self, group, version, namespace, plural, name, body, **kwargs):
        all_params = ['group', 'version', 'namespace', 'plural', 'name', 'body']
        all_params.append('async')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method update_namespaced_custom_object" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'group' is set
        if ('group' not in params) or (params['group'] is None):
            raise ValueError("Missing the required parameter `group` when calling `update_namespaced_custom_object`")
        # verify the required parameter 'version' is set
        if ('version' not in params) or (params['version'] is None):
            raise ValueError("Missing the required parameter `version` when calling `update_namespaced_custom_object`")
        # verify the required parameter 'namespace' is set
        if ('namespace' not in params) or (params['namespace'] is None):
            raise ValueError("Missing the required parameter `namespace` when calling `update_namespaced_custom_object`")
        # verify the required parameter 'plural' is set
        if ('plural' not in params) or (params['plural'] is None):
            raise ValueError("Missing the required parameter `plural` when calling `update_namespaced_custom_object`")
        # verify the required parameter 'name' is set
        if ('name' not in params) or (params['name'] is None):
            raise ValueError("Missing the required parameter `name` when calling `update_namespaced_custom_object`")
        # verify the required parameter 'body' is set
        if ('body' not in params) or (params['body'] is None):
            raise ValueError("Missing the required parameter `body` when calling `update_namespaced_custom_object`")

        collection_formats = {}

        resource_path = '/apis/{group}/{version}/namespaces/{namespace}/{plural}/{name}'.replace('{format}', 'json')
        path_params = {}
        if 'group' in params:
            path_params['group'] = params['group']
        if 'version' in params:
            path_params['version'] = params['version']
        if 'namespace' in params:
            path_params['namespace'] = params['namespace']
        if 'plural' in params:
            path_params['plural'] = params['plural']
        if 'name' in params:
            path_params['name'] = params['name']

        query_params = {}

        header_params = {}

        form_params = []
        local_var_files = {}

        body_params = None
        if 'body' in params:
            body_params = params['body']
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.\
            select_header_accept(['application/json'])

        # HTTP header `Content-Type`
        header_params['Content-Type'] = self.api_client.\
            select_header_content_type(['application/merge-patch+json', 'application/strategic-merge-patch+json'])

        # Authentication setting
        auth_settings = ['BearerToken']

        return self.api_client.call_api(resource_path, 'PATCH',
                                        path_params,
                                        query_params,
                                        header_params,
                                        body=body_params,
                                        post_params=form_params,
                                        files=local_var_files,
                                        response_type='object',
                                        auth_settings=auth_settings,
                                        async=params.get('async'),
                                        _return_http_data_only=params.get('_return_http_data_only'),
                                        _preload_content=params.get('_preload_content', True),
                                        _request_timeout=params.get('_request_timeout'),
                                        collection_formats=collection_formats)
