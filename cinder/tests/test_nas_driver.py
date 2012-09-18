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
"""Unit tests for the NFS driver module"""
import os
import mox
from cinder import test
from cinder import exception
from cinder import context
from cinder import flags
from cinder.openstack.common import log as logging
from cinder.volume import nas_driver

from cinder.tests import fake_utils

from cinder.db.sqlalchemy import models

FLAGS = flags.FLAGS
LOG = logging.getLogger(__name__)


fake_volume = {'id': 'qwerty',
               'name': 'volume-00002',
               'provider_location': '/mnt/nfs/volume-00002'}

fake_share_nfs = {'proto': 'NFSV3',
                  'export_location': '127.0.0.1:/mnt/nfs/volume-00002'}

fake_share_cifs = {'proto': 'CIFS',
                   'export_location': '//127.0.0.1/volume-00002'}

fake_access_values = [
        {'id': 0, 'access_type': 'ip', 'state': 'active'},
        {'id': 1, 'access_type': 'ip', 'state': 'error'}]
fake_access_list = []
for item in fake_access_values:
    fake_access_list.append(models.ShareAccessMapping())
    fake_access_list[-1].update(item)


class NfsDriverTestCase(test.TestCase):
    """Test case for NFS driver"""

    def setUp(self):
        super(NfsDriverTestCase, self).setUp()
        #remember executor
        self._execute = fake_utils.fake_execute
        fake_utils.fake_execute_clear_log()
        self._context = context.get_admin_context()

        #common preset
        self.mox.StubOutClassWithMocks(nas_driver, 'NFSHelper')
        self.mox.StubOutClassWithMocks(nas_driver, 'CIFSHelper')

        self._helper_cifs = nas_driver.CIFSHelper(self._execute,
                                                  FLAGS.smb_config_path)
        self._helper_nfs = nas_driver.NFSHelper(self._execute)

    def tearDown(self):
        super(NfsDriverTestCase, self).tearDown()

    def test_do_setup(self):
        self.mox.StubOutWithMock(nas_driver.driver.VolumeDriver, 'do_setup')
        nas_driver.driver.VolumeDriver.do_setup(self._context)
        self._helper_nfs.init()
        self._helper_cifs.init()

        self.mox.ReplayAll()
        driver = nas_driver.ShareDriver(self._execute)
        driver.do_setup(self._context)

    def test_create_volume(self):
        self.mox.StubOutWithMock(nas_driver.driver.VolumeDriver,
                                 'create_volume')
        nas_driver.driver.VolumeDriver.create_volume(fake_volume)

        self.mox.ReplayAll()
        driver = nas_driver.ShareDriver(self._execute)
        driver.create_volume(fake_volume)

        expected_exec = ['mkfs.ext4 ' + driver.local_path(fake_volume)]
        self.assertEqual(fake_utils.fake_execute_get_log(), expected_exec)

    def test_create_export(self):
        ret = self.test_ensure_export()
        mount_path = self._get_mount_path(fake_volume)
        self.assertEqual(ret, {'provider_location': mount_path})

    def test_ensure_export(self):
        self.mox.ReplayAll()
        driver = nas_driver.ShareDriver(self._execute)
        ret = driver.create_export(self._context, fake_volume)

        device_name = driver.local_path(fake_volume)
        mount_path = self._get_mount_path(fake_volume)
        expected_exec = ['mkdir -p %s' % mount_path,
                         'mount %s %s' % (device_name, mount_path),
                         'chmod 777 %s' % mount_path]
        self.assertEqual(fake_utils.fake_execute_get_log(), expected_exec)
        return ret

    def test_remove_export(self):
        mount_path = self._get_mount_path(fake_volume)
        self.mox.StubOutWithMock(nas_driver.os.path, 'exists')
        self.mox.StubOutWithMock(nas_driver.os, 'rmdir')
        nas_driver.os.path.exists(mount_path).AndReturn(True)
        nas_driver.os.rmdir(mount_path)

        self.mox.ReplayAll()
        driver = nas_driver.ShareDriver(self._execute)
        driver.remove_export(self._context, fake_volume)

        expected_exec = ['umount -f %s' % mount_path]
        self.assertEqual(fake_utils.fake_execute_get_log(), expected_exec)

    def test_create_share(self):
        mount_path = self._get_mount_path(fake_volume)
        self._helper_nfs.create_export(mount_path, fake_volume['name']).\
            AndReturn(fake_share_nfs['export_location'])
        self.mox.ReplayAll()

        driver = nas_driver.ShareDriver(self._execute)
        driver.create_share(self._context, fake_share_nfs, fake_volume)

    def test_ensure_share(self):
        mount_path = self._get_mount_path(fake_volume)
        self._helper_nfs.create_export(mount_path, fake_volume['name'],
                                       recreate=True)
        db = self.mox.CreateMockAnything()
        db.share_access_get_all_for_share(self._context, fake_volume['id']).\
            AndReturn(fake_access_list)
        self.mox.StubOutWithMock(nas_driver.ShareDriver, 'access_allow')
        nas_driver.ShareDriver.access_allow(self._context, fake_share_nfs,
                                            fake_volume,
                                            fake_access_list[0])

        self.mox.ReplayAll()

        driver = nas_driver.ShareDriver(self._execute)
        driver.db = db
        driver.ensure_share(self._context, fake_share_nfs, fake_volume)

    def test_delete_share(self):
        mount_path = self._get_mount_path(fake_volume)

        db = self.mox.CreateMockAnything()
        db.share_access_get_all_for_share(self._context, fake_volume['id']).\
        AndReturn(fake_access_list)

        self.mox.StubOutWithMock(nas_driver.ShareDriver, 'access_deny')
        nas_driver.ShareDriver.access_deny(self._context,
                                           fake_share_nfs,
                                           fake_volume,
                                           fake_access_list[0])
        nas_driver.ShareDriver.access_deny(self._context,
                                           fake_share_nfs,
                                           fake_volume,
                                           fake_access_list[1])

        self._helper_nfs.remove_export(mount_path, fake_volume['name'])
        self.mox.ReplayAll()

        driver = nas_driver.ShareDriver(self._execute)
        driver.db = db
        driver.delete_share(self._context, fake_share_nfs, fake_volume)

    def test_access_allow(self):
        mount_path = self._get_mount_path(fake_volume)
        self._helper_cifs.access_allow(mount_path,
                                       fake_volume['name'],
                                       fake_access_list[0]['access_type'],
                                       fake_access_list[0]['access_to'])

        self.mox.ReplayAll()

        driver = nas_driver.ShareDriver(self._execute)
        driver.access_allow(self._context, fake_share_cifs, fake_volume,
                            fake_access_list[0])

    def test_access_deny(self):
        mount_path = self._get_mount_path(fake_volume)
        self._helper_cifs.access_deny(mount_path,
                                       fake_volume['name'],
                                       fake_access_list[0]['access_type'],
                                       fake_access_list[0]['access_to'])

        self.mox.ReplayAll()

        driver = nas_driver.ShareDriver(self._execute)
        driver.access_deny(self._context, fake_share_cifs, fake_volume,
                            fake_access_list[0])

    def _get_mount_path(self, volume):
        return os.path.join(FLAGS.share_export_root, volume['name'])


class NFSHelperTestCase(test.TestCase):
    """Test case for NFS driver"""

    def setUp(self):
        super(NFSHelperTestCase, self).setUp()
        FLAGS.share_export_ip = '127.0.0.1'
        #remember executor
        self._execute = fake_utils.fake_execute
        self._helper = nas_driver.NFSHelper(self._execute)
        fake_utils.fake_execute_clear_log()

    def tearDown(self):
        super(NFSHelperTestCase, self).tearDown()

    def test_create_export(self):
        self.mox.ReplayAll()
        ret = self._helper.create_export('/opt/nfs', 'volume-00001')
        expected_location = '%s:/opt/nfs' % FLAGS.share_export_ip
        self.assertEqual(ret, expected_location)

    def test_remove_export(self):
        self.mox.ReplayAll()

    def test_access_allow(self):
        self.mox.ReplayAll()
        self._helper.access_allow('/opt/nfs', 'volume-00001', 'ip', '10.0.0.*')

        export_string = '10.0.0.*:/opt/nfs'
        expected_exec = ['exportfs',
                         'exportfs -o rw,no_subtree_check %s' % export_string]
        self.assertEqual(fake_utils.fake_execute_get_log(), expected_exec)

    def test_access_allow_negative(self):
        def exec_runner(*ignore_args, **ignore_kwargs):
            return '\n/opt/nfs\t\t10.0.0.*\n', ''
        fake_utils.fake_execute_set_repliers([('[.]*', exec_runner)])
        self.mox.ReplayAll()
        self.assertRaises(exception.ShareAccessExists,
                          self._helper.access_allow,
                          '/opt/nfs', 'volume-00001', 'ip', '10.0.0.*')

    def test_access_deny(self):
        self.mox.ReplayAll()
        self._helper.access_deny('/opt/nfs', 'volume-00001', 'ip', '10.0.0.*')

        export_string = '10.0.0.*:/opt/nfs'
        expected_exec = ['exportfs -u %s' % export_string]
        self.assertEqual(fake_utils.fake_execute_get_log(), expected_exec)


class OpenMock(object):

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass


class CIFSHelperTestCase(test.TestCase):
    """Test case for NFS driver"""

    def setUp(self):
        super(CIFSHelperTestCase, self).setUp()
        FLAGS.share_export_ip = '127.0.0.1'

        self.conf_path = '/opt/smb.cfs'
        self.mount_path = '/opt/nfs/volume-0001'
        self.share_name = 'volume-0001'

        self.mox.StubOutClassWithMocks(nas_driver.ConfigParser, 'ConfigParser')
        #remember executor
        self._execute = fake_utils.fake_execute
        self._helper = nas_driver.CIFSHelper(self._execute, self.conf_path)

        fake_utils.fake_execute_clear_log()
        self.mox.ResetAll()
        nas_driver.open = OpenMock
#        nas_driver.open.__enter__()
#        nas_driver.open.__exit__(None, None, None)

    def tearDown(self):
        del nas_driver.open
        super(CIFSHelperTestCase, self).tearDown()

    def test_create_export(self):
        parser = nas_driver.ConfigParser.ConfigParser()
        parser.read(self.conf_path)
        parser.has_section(self.share_name).AndReturn(False)
        parser.add_section(self.share_name)
        parser.set(self.share_name, 'path', self.mount_path)
        parser.set(self.share_name, 'browseable', 'yes')
        parser.set(self.share_name, 'guest ok', 'yes')
        parser.set(self.share_name, 'read only', 'no')
        parser.set(self.share_name, 'writable', 'yes')
        parser.set(self.share_name, 'create mask', '0755')
        parser.set(self.share_name, 'hosts deny', '0.0.0.0/0')
        parser.set(self.share_name, 'hosts allow', '127.0.0.1')
        parser.write(mox.IgnoreArg())
        parser.write(mox.IgnoreArg())

        self.mox.ReplayAll()
        ret = self._helper.create_export(self.mount_path, self.share_name)

        expected_location = '//%s/%s' % (FLAGS.share_export_ip,
                                         self.share_name)
        expected_exec = ['chown nobody -R %s' % self.mount_path,
                         'testparm -s %s' % self._helper.test_config,
                         'pkill -HUP smbd']
        self.assertEqual(fake_utils.fake_execute_get_log(), expected_exec)
        self.assertEqual(ret, expected_location)

    def test_remove_export(self):
        parser = nas_driver.ConfigParser.ConfigParser()
        parser.read(self.conf_path)
        parser.has_section(self.share_name).AndReturn(True)
        parser.remove_section(self.share_name)
        parser.write(mox.IgnoreArg())
        parser.write(mox.IgnoreArg())

        self.mox.ReplayAll()
        self._helper.remove_export(self.mount_path, self.share_name)

        expected_exec = ['testparm -s %s' % self._helper.test_config,
                         'pkill -HUP smbd',
                         'smbcontrol all close-share volume-0001',
                         ]
        self.assertEqual(fake_utils.fake_execute_get_log(), expected_exec)

    def test_access_allow(self):
        parser = nas_driver.ConfigParser.ConfigParser()
        parser.read(self.conf_path)
        parser.get(self.share_name, 'hosts allow').AndReturn('127.0.0.1')
        parser.set(self.share_name, 'hosts allow', '127.0.0.1 10.0.0.*')
        parser.write(mox.IgnoreArg())
        parser.write(mox.IgnoreArg())

        self.mox.ReplayAll()
        self._helper.access_allow(self.mount_path, self.share_name,
                                  'ip', '10.0.0.*')

        expected_exec = ['testparm -s %s' % self._helper.test_config,
                         'pkill -HUP smbd']
        self.assertEqual(fake_utils.fake_execute_get_log(), expected_exec)

    def test_access_deny(self):
        parser = nas_driver.ConfigParser.ConfigParser()
        parser.read(self.conf_path)
        parser.get(self.share_name, 'hosts allow').\
            AndReturn('127.0.0.1 10.0.0.* 10.0.1.*')
        parser.set(self.share_name, 'hosts allow', '127.0.0.1 10.0.1.*')
        parser.write(mox.IgnoreArg())
        parser.write(mox.IgnoreArg())

        self.mox.ReplayAll()
        self._helper.access_deny(self.mount_path, self.share_name,
                                  'ip', '10.0.0.*')

        expected_exec = [
            'testparm -s %s' % self._helper.test_config,
            'pkill -HUP smbd']
        self.assertEqual(fake_utils.fake_execute_get_log(), expected_exec)
