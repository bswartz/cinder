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

"""The volumes snapshots api."""

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


def _translate_snapshot_detail_view(context, snapshot, share):
    """Maps keys for snapshots details view."""

    d = _translate_snapshot_summary_view(context, snapshot, share)

    # NOTE(gagupta): No additional data / lookups at the moment
    return d


def _translate_snapshot_summary_view(context, snapshot, share):
    """Maps keys for snapshots summary view."""
    d = {}

    d['id'] = snapshot['id']
    d['created_at'] = snapshot['created_at']
    d['display_name'] = snapshot['display_name']
    d['display_description'] = snapshot['display_description']
    d['volume_id'] = snapshot['volume_id']
    d['status'] = snapshot['status']
    d['size'] = snapshot['volume_size']

    d['source_type'] = 'volume' if share is None else share.proto
    return d


def make_snapshot(elem):
    elem.set('id')
    elem.set('status')
    elem.set('size')
    elem.set('created_at')
    elem.set('display_name')
    elem.set('display_description')
    elem.set('volume_id')
    elem.set('source_type')


class SnapshotTemplate(xmlutil.TemplateBuilder):
    def construct(self):
        root = xmlutil.TemplateElement('snapshot', selector='snapshot')
        make_snapshot(root)
        return xmlutil.MasterTemplate(root, 1)


class SnapshotsTemplate(xmlutil.TemplateBuilder):
    def construct(self):
        root = xmlutil.TemplateElement('snapshots')
        elem = xmlutil.SubTemplateElement(root, 'snapshot',
                                          selector='snapshots')
        make_snapshot(elem)
        return xmlutil.MasterTemplate(root, 1)


class SnapshotsController(object):
    """The Volumes API controller for the OpenStack API."""

    def __init__(self):
        self.volume_api = volume.API()
        super(SnapshotsController, self).__init__()

    @wsgi.serializers(xml=SnapshotTemplate)
    def show(self, req, id):
        """Return data about the given snapshot."""
        context = req.environ['cinder.context']

        try:
            snap = self.volume_api.get_snapshot(context, id)
            try:
                share, _ = self.volume_api.get_share_volume(context,
                                                            snap['volume_id'])
            except exception.NotFound:
                share = None
        except exception.NotFound:
            raise exc.HTTPNotFound()

        return {'snapshot': _translate_snapshot_detail_view(context, snap,
                                                            share)}

    def delete(self, req, id):
        """Delete a snapshot."""
        context = req.environ['cinder.context']

        LOG.audit(_("Delete snapshot with id: %s"), id, context=context)

        try:
            snapshot = self.volume_api.get_snapshot(context, id)
            self.volume_api.delete_snapshot(context, snapshot)
        except exception.NotFound:
            raise exc.HTTPNotFound()
        return webob.Response(status_int=202)

    @wsgi.serializers(xml=SnapshotsTemplate)
    def index(self, req):
        """Returns a summary list of snapshots."""
        return self._items(req, entity_maker=_translate_snapshot_summary_view)

    @wsgi.serializers(xml=SnapshotsTemplate)
    def detail(self, req):
        """Returns a detailed list of snapshots."""
        return self._items(req, entity_maker=_translate_snapshot_detail_view)

    def _items(self, req, entity_maker):
        """Returns a list of snapshots, transformed through entity_maker."""
        context = req.environ['cinder.context']

        search_opts = {}
        search_opts.update(req.GET)

        snapshots = self.volume_api.get_all_snapshots(context,
                                                      search_opts=search_opts)
        svlist = self.volume_api.get_all_shares_volumes(context)
        shares = dict([(sv[0]['volume_id'], sv[0]) for sv in svlist])

        limited_list = common.limited(snapshots, req)
        res = [entity_maker(context, snapshot,
                            shares.get(snapshot['volume_id'], None))
               for snapshot in limited_list]
        return {'snapshots': res}

    @wsgi.serializers(xml=SnapshotTemplate)
    def create(self, req, body):
        """Creates a new snapshot."""
        context = req.environ['cinder.context']

        if not body:
            return exc.HTTPUnprocessableEntity()

        snapshot = body['snapshot']
        volume_id = snapshot['volume_id']
        volume = self.volume_api.get(context, volume_id)
        force = snapshot.get('force', False)
        msg = _("Create snapshot from volume %s")
        LOG.audit(msg, volume_id, context=context)

        if force:
            new_snapshot = self.volume_api.create_snapshot_force(context,
                                        volume,
                                        snapshot.get('display_name'),
                                        snapshot.get('display_description'))
        else:
            new_snapshot = self.volume_api.create_snapshot(context,
                                        volume,
                                        snapshot.get('display_name'),
                                        snapshot.get('display_description'))
        try:
            share, vol = self.volume_api.get_share_volume(context, volume_id)
        except exception.NotFound:
            share = None
        retval = _translate_snapshot_detail_view(context, new_snapshot, share)

        return {'snapshot': retval}


def create_resource():
    return wsgi.Resource(SnapshotsController())
