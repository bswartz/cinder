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

import cStringIO

import mox

from cinder import context
from cinder import db
from cinder import flags
from cinder.openstack.common import log as logging
from cinder import test
from cinder.volume import nas_manager
from cinder.volume.manager import VolumeManager
from cinder.volume.nas_driver import ShareDriver


FLAGS = flags.FLAGS
LOG = logging.getLogger(__name__)


class ShareTestCase(test.TestCase):
    """Test Case for Shares"""
    def setUp(self):
        super(ShareTestCase, self).setUp()
        self.flags(connection_type='fake')
        self.volume = nas_manager.NasVolumeManager(
            volume_driver="cinder.volume.nas_driver.FakeShareDriver")
        self.driver = ShareDriver
        self.ext = nas_manager.NasActionExtension
        self.context = context.get_admin_context()

    def _fake_share_access_get(self, access_id):
        class Access(object):

            def __init__(self, **kwargs):
                self.STATE_NEW = 'fake_new'
                self.STATE_ACTIVE = 'fake_active'
                self.STATE_ERROR = 'fake_error'
                self.params = kwargs
                self.params['state'] = self.STATE_NEW

            def __getitem__(self, item):
                return self.params[item]

        access = Access(access_id=access_id, volume_id='fake_vol_id')
        return access

    def test_create_delete_share(self):
        self.mox.StubOutWithMock(db, 'share_get_by_volume_id')
        self.mox.StubOutWithMock(VolumeManager, 'create_volume')
        self.mox.StubOutWithMock(VolumeManager, 'delete_volume')

        db.share_get_by_volume_id(self.context, 1)
        VolumeManager.create_volume(self.context, 1,
            snapshot_id=None,
            action_extension=mox.IsA(self.ext))

        db.share_get_by_volume_id(self.context, 1)
        VolumeManager.delete_volume(self.context, 1,
            action_extension=mox.IsA(self.ext))
        self.mox.ReplayAll()
        self.volume.create_share(self.context, 1)
        self.volume.delete_share(self.context, 1)

    def test_allow_deny_access(self):
        self.mox.StubOutWithMock(db, 'share_access_get')
        self.mox.StubOutWithMock(db, 'share_volume_get')
        self.mox.StubOutWithMock(db, 'share_access_update')
        self.mox.StubOutWithMock(self.driver, 'access_allow')
        self.mox.StubOutWithMock(self.driver, 'access_deny')

        fake_access_id = 'fake_access_id'
        fake_access = self._fake_share_access_get(fake_access_id)
        fake_sh_vol = ('fake_sh', 'fake_vol')
        db.share_access_get(self.context,
            fake_access_id).AndReturn(fake_access)
        db.share_volume_get(self.context, 'fake_vol_id').AndReturn(fake_sh_vol)
        self.driver.access_allow(self.context,
                                'fake_sh',
                                'fake_vol',
                                fake_access)
        db.share_access_update(self.context, fake_access_id,
                {'state': 'fake_active'})
        db.share_access_get(self.context,
            fake_access_id).AndReturn(fake_access)
        db.share_volume_get(self.context, 'fake_vol_id').AndReturn(fake_sh_vol)
        self.driver.access_deny(self.context,
            'fake_sh',
            'fake_vol',
            fake_access)
        self.mox.ReplayAll()
        self.volume.allow_access(self.context, fake_access_id)
        self.volume.deny_access(self.context, fake_access_id)

    def test_allow_deny_access_error(self):
        self.mox.StubOutWithMock(db, 'share_access_get')
        self.mox.StubOutWithMock(db, 'share_volume_get')
        self.mox.StubOutWithMock(db, 'share_access_update')
        self.mox.StubOutWithMock(self.driver, 'access_allow')
        self.mox.StubOutWithMock(self.driver, 'access_deny')

        fake_access_id = 'fake_access_id'
        fake_access = self._fake_share_access_get(fake_access_id)
        fake_sh_vol = ('fake_sh', 'fake_vol')
        db.share_access_get(self.context,
            fake_access_id).AndReturn(fake_access)
        db.share_volume_get(self.context, 'fake_vol_id').AndReturn(fake_sh_vol)
        self.driver.access_allow(self.context,
            'fake_sh',
            'fake_vol',
            fake_access).AndRaise(Exception())
#        db.share_access_update(self.context, fake_access_id,
#                {'state': 'fake_error'})
        db.share_access_get(self.context,
            fake_access_id).AndReturn(fake_access)
        db.share_volume_get(self.context, 'fake_vol_id').AndReturn(fake_sh_vol)
        self.driver.access_deny(self.context,
            'fake_sh',
            'fake_vol',
            fake_access).AndRaise(Exception())
#        db.share_access_update(self.context, fake_access_id,
#                {'state': 'fake_error'})
        self.mox.ReplayAll()
        self.assertRaises(Exception, self.volume.allow_access, self.context,
            fake_access_id)
        self.assertRaises(Exception, self.volume.deny_access, self.context,
            fake_access_id)
