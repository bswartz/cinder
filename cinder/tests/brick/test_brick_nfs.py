# vim: tabstop=4 shiftwidth=4 softtabstop=4

# (c) Copyright 2013 OpenStack Foundation.
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

import mox

from cinder.brick.nfs import nfs
from cinder.openstack.common import log as logging
from cinder.volume import configuration as conf
from cinder.openstack.common import processutils as putils
from cinder import test

LOG = logging.getLogger(__name__)


class BrickNfsTestCase(test.TestCase):
    TEST_EXPORT = '1.2.3.4/export1'
    TEST_MNT_BASE = '/mnt/test'
    TEST_HASH = '4d664fd43b6ff86d80a4ea969c07b3b9'
    TEST_MNT_POINT = TEST_MNT_BASE + '/' + TEST_HASH

    def setUp(self):
        super(BrickNfsTestCase, self).setUp()
        self._mox = mox.Mox()
        self.configuration = mox.MockObject(conf.Configuration)
        self.configuration.append_config_values(mox.IgnoreArg())
        self.configuration.nfs_mount_options = None
        self.configuration.nfs_mount_point_base = self.TEST_MNT_BASE
        self._client = nfs.NfsClient(configuration=self.configuration)
        self.addCleanup(self._mox.UnsetStubs)

    def test_get_hash_str(self):
        """_get_hash_str should calculation correct value."""

        self.assertEqual(self.TEST_HASH,
                         self._client._get_hash_str(self.TEST_EXPORT))

    def test_get_mount_point(self):
        mnt_point = self._client.get_mount_point(self.TEST_EXPORT)
        self.assertEqual(mnt_point, self.TEST_MNT_POINT)

    def test_mount_nfs_should_mount_correctly(self):
        mox = self._mox
        client = self._client

        mox.StubOutWithMock(client, '_execute')
        client._execute('mount', check_exit_code=0).AndReturn(("", ""))
        client._execute('mkdir', '-p', self.TEST_MNT_POINT,
                        check_exit_code=0).AndReturn(("", ""))
        client._execute('mount', '-t', 'nfs', self.TEST_EXPORT,
                        self.TEST_MNT_POINT,
                        root_helper='sudo', run_as_root=True,
                        check_exit_code=0).AndReturn(("", ""))
        mox.ReplayAll()

        client.mount(self.TEST_EXPORT)

        mox.VerifyAll()

    def test_mount_nfs_should_not_remount(self):
        mox = self._mox
        client = self._client

        line = "%s on %s type nfs (rw)\n" % (self.TEST_EXPORT,
                                             self.TEST_MNT_POINT)
        mox.StubOutWithMock(client, '_execute')
        client._execute('mount', check_exit_code=0).AndReturn((line, ""))
        mox.ReplayAll()

        client.mount(self.TEST_EXPORT)

        mox.VerifyAll()
