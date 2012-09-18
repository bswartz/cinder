# Copyright 2011 Justin Santa Barbara
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""The shares api."""

from webob import exc
import webob

from cinder.api.openstack import common
from cinder.api.openstack import wsgi
from cinder.api.openstack import xmlutil
from cinder import exception
from cinder import flags
from cinder.openstack.common import log as logging
from cinder import volume


LOG = logging.getLogger(__name__)


FLAGS = flags.FLAGS


def _translate_share_detail_view(context, share, volume):
    """Maps keys for shares details view."""

    d = _translate_share_summary_view(context, share, volume)

    # No additional data / lookups at the moment

    return d


def _translate_share_summary_view(context, share, volume):
    """Maps keys for shares summary view."""
    d = dict()

    d['id'] = volume['id']
    d['share_type'] = share['proto']
    d['export_location'] = share['export_location']
    d['status'] = volume['status']
    d['display_name'] = volume['display_name']
    d['size'] = volume['size']

    LOG.audit("share=%s", share, context=context)
    return d


def make_share(elem):
    elem.set('id')
    elem.set('proto')
    elem.set('export_location')

share_nsmap = {None: xmlutil.XMLNS_SHARE_V1, 'atom': xmlutil.XMLNS_ATOM}


class ShareTemplate(xmlutil.TemplateBuilder):
    def construct(self):
        root = xmlutil.TemplateElement('share', selector='share')
        make_share(root)
        return xmlutil.MasterTemplate(root, 1, nsmap=share_nsmap)


class SharesTemplate(xmlutil.TemplateBuilder):
    def construct(self):
        root = xmlutil.TemplateElement('shares')
        elem = xmlutil.SubTemplateElement(root, 'share', selector='shares')
        make_share(elem)
        return xmlutil.MasterTemplate(root, 1, nsmap=share_nsmap)


class ShareController(object):
    """The Shares API controller for the OpenStack API."""

    def __init__(self):
        self.share_api = volume.API()
        super(ShareController, self).__init__()

    @wsgi.serializers(xml=ShareTemplate)
    def show(self, req, id):
        """Return data about the given share."""
        context = req.environ['cinder.context']

        try:
            share, volume = self.share_api.get_share_volume(context, id)
        except exception.NotFound:
            raise exc.HTTPNotFound()

        return {'share': _translate_share_detail_view(context, share, volume)}

    def delete(self, req, id):
        """Delete a share."""
        context = req.environ['cinder.context']

        LOG.audit("Delete share with id: %s", id, context=context)

        try:
            _, volume = self.share_api.get_share_volume(context, id)
            self.share_api.delete(context, volume)
        except exception.NotFound:
            raise exc.HTTPNotFound()
        return webob.Response(status_int=202)

    @wsgi.serializers(xml=ShareTemplate)
    def index(self, req):
        """Returns a summary list of shares."""
        return self._items(req, entity_maker=_translate_share_summary_view)

    @wsgi.serializers(xml=ShareTemplate)
    def detail(self, req):
        """Returns a detailed list of volumes."""
        return self._items(req, entity_maker=_translate_share_detail_view)

    def _items(self, req, entity_maker):
        """Returns a list of shares, transformed through entity_maker."""
        context = req.environ['cinder.context']

        sv_list = self.share_api.get_all_shares_volumes(context)
        limited_list = common.limited(sv_list, req)
        res = [entity_maker(context, share, vol) for share, vol in sv_list]
        return {'shares': res}

    @wsgi.serializers(xml=ShareTemplate)
    def create(self, req, body):
        """Creates a new share."""
        context = req.environ['cinder.context']

        if not body:
            raise exc.HTTPUnprocessableEntity()

        share = body['share']
        size = share['size']
        proto = share['proto'].upper()

        LOG.audit("Create %s share of %s GB",
                  proto.upper(),
                  size,
                  context=context)

        kwargs = {}

        snapshot_id = share.get('snapshot_id')
        if snapshot_id is not None:
            kwargs['snapshot'] = self.share_api.get_snapshot(context,
                                                             snapshot_id)
        else:
            kwargs['snapshot'] = None

        kwargs['availability_zone'] = share.get('availability_zone', None)

        new_share, new_volume = self.share_api.create_share(context,
                                            proto,
                                            size,
                                            share.get('display_name'),
                                            share.get('display_description'),
                                            **kwargs)

        # TODO(vish): Instance should be None at db layer instead of
        #             trying to lazy load, but for now we turn it into
        #             a dict to avoid an error.
        retval = _translate_share_detail_view(context, new_share, new_volume)

        return {'share': retval}


def create_resource():
    return wsgi.Resource(ShareController())
