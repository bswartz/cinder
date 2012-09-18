# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2012 NetApp
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
"""
NAS volume manager manages creating NAS storages and access rules
"""

from cinder import context
from cinder import flags
from cinder.openstack.common import log as logging
from cinder.openstack.common import excutils
from cinder.volume import manager
from cinder import exception

LOG = logging.getLogger(__name__)

FLAGS = flags.FLAGS


class NasActionExtension(manager.DriverActionExtension):
    """Class that can be used call extended functionality for driver
    on create, ensure and delete step
    """
    def __init__(self, db, share, driver):
        self.db = db
        self.share = share
        self.driver = driver

    def create(self, context, volume):
        export_location = self.driver.create_share(context, self.share, volume)
        if export_location is not None:
            self.db.share_update(context, self.share['id'],
                    {'export_location': export_location})

    def ensure(self, context, volume):
        self.driver.ensure_share(context, self.share, volume)

    def delete(self, context, volume):
        self.driver.delete_share(context, self.share, volume)

    def destroy(self, context, volume):
        self.db.share_delete(context, self.share['id'])


class NasVolumeManager(manager.VolumeManager):
    """Manages NAS storages."""

    def init_host(self):
        """Do any initialization that needs to be run if this is a
           standalone service."""
        super(NasVolumeManager, self).init_host()
        ctxt = context.get_admin_context()
        svlist = self.db.shares_volume_get_all_by_host(ctxt, self.host)
        LOG.debug(_("Re-exporting %s shares"), len(svlist))
        for share, volume in svlist:
            if volume['status'] in ['available', 'in-use']:
                self.driver.ensure_share(ctxt, share, volume)
            else:
                LOG.info(_("volume %s: skipping export"), volume['name'])

    def create_volume(self, context, volume_id, *args, **kwargs):
        volume_ref = self.db.volume_get(context, volume_id)
        self.db.volume_update(context,
                              volume_ref['id'], {'status': 'error'})
        raise exception.CinderException('Only share creation is supported')

    def delete_volume(self, context, volume_id):
        try:
            self.db.share_get_by_volume_id(context, volume_id)
            self.delete_share(context, volume_id)
        except exception.NotFound:
            super(NasVolumeManager, self).delete_volume(context, volume_id)

    def attach_volume(self, *args, **kwargs):
        raise NotImplementedError()

    def detach_volume(self, *args, **kwargs):
        raise NotImplementedError()

    def initialize_connection(self, *args, **kwargs):
        raise NotImplementedError()

    def terminate_connection(self, *args, **kwargs):
        raise NotImplementedError()

    def create_share(self, context, volume_id, snapshot_id=None):
        """
        Create new share based on some volume
        """
        share_ref = self.db.share_get_by_volume_id(context, volume_id)
        ext = NasActionExtension(self.db, share_ref, self.driver)
        super(NasVolumeManager, self).create_volume(context, volume_id,
                                                    snapshot_id=snapshot_id,
                                                    action_extension=ext)

    def delete_share(self, context, volume_id):
        """ Delete a share new share based on some volume """
        share_ref = self.db.share_get_by_volume_id(context, volume_id)
        ext = NasActionExtension(self.db, share_ref, self.driver)
        return super(NasVolumeManager, self).\
            delete_volume(context, volume_id, action_extension=ext)

    def allow_access(self, context, access_id):
        """"Allow connection to volume (share type)"""
        try:
            access_ref = self.db.share_access_get(context, access_id)
            share_ref, volume_ref = \
                self.db.share_volume_get(context, access_ref['volume_id'])
            #state could be deleting
            if access_ref["state"] == access_ref.STATE_NEW:
                self.driver.access_allow(context, share_ref,
                                         volume_ref, access_ref)
                self.db.share_access_update(context, access_id,
                        {'state': access_ref.STATE_ACTIVE})
        except Exception:
            with excutils.save_and_reraise_exception():
                self.db.share_access_update(context, access_id,
                        {'state': access_ref.STATE_ERROR})

    def deny_access(self, context, access_id):
        """"Deny connection to volume (share type)"""
        try:
            access_ref = self.db.share_access_get(context, access_id)
            share_ref, volume_ref = \
                self.db.share_volume_get(context, access_ref['volume_id'])
            self.driver.access_deny(context, share_ref, volume_ref, access_ref)
        except Exception:
            with excutils.save_and_reraise_exception():
                self.db.share_access_update(context, access_id,
                        {'state': access_ref.STATE_ERROR})
        self.db.share_access_delete(context, access_id)
