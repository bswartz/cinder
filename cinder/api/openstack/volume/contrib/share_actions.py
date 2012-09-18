#   Copyright 2012 NetApp.
#
#   Licensed under the Apache License, Version 2.0 (the "License"); you may
#   not use this file except in compliance with the License. You may obtain
#   a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#   License for the specific language governing permissions and limitations
#   under the License.

import webob
from webob import exc

from cinder.api.openstack import extensions
from cinder.api.openstack import wsgi
from cinder import volume
from cinder import flags
from cinder.openstack.common import log as logging
from cinder import exception

FLAGS = flags.FLAGS
LOG = logging.getLogger(__name__)


def authorize(context, action_name):
    action = 'share_actions:%s' % action_name
    extensions.extension_authorizer('volume', action)(context)


class ShareActionsController(wsgi.Controller):
    def __init__(self, *args, **kwargs):
        super(ShareActionsController, self).__init__(*args, **kwargs)
        self.volume_api = volume.API()

    @wsgi.action('os-access_allow')
    def _access_allow(self, req, id, body):
        """Add share access rule."""
        context = req.environ['cinder.context']
        volume = self.volume_api.get(context, id)

        access_type = body['os-access_allow']['access_type']
        access_to = body['os-access_allow']['access_to']

        access = self.volume_api.access_allow(context, volume, access_type,
                                              access_to)
        return webob.Response(status_int=202)

    @wsgi.action('os-access_deny')
    def _access_deny(self, req, id, body):
        """Initialize volume attachment."""
        context = req.environ['cinder.context']
        access_id = body['os-access_deny']['access_id']

        try:
            access = self.volume_api.access_get(context, access_id)
            if access.volume_id != id:
                raise exception.NotFound()
            volume = self.volume_api.get(context, id)
        except exception.NotFound:
            raise exc.HTTPNotFound()
        self.volume_api.access_deny(context, volume, access)
        return webob.Response(status_int=202)

    @wsgi.action('os-access_list')
    def _access_list(self, req, id, body):
        """list access rules."""
        context = req.environ['cinder.context']
        volume = self.volume_api.get(context, id)
        access_list = self.volume_api.access_get_all(context, volume)
        return {'access_list': access_list}


class Share_actions(extensions.ExtensionDescriptor):
    """Enable volume actions
    """

    name = "ShareActions"
    alias = "os-share-actions"
    namespace = ""
    updated = "2012-08-14T00:00:00+00:00"

    def get_controller_extensions(self):
        controller = ShareActionsController()
        extension = extensions.ControllerExtension(self, 'shares', controller)
        return [extension]
