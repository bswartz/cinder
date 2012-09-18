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

"""Unit tests for the NetApp NAS driver module"""
from cinder import context
from cinder import test
from cinder import exception
from cinder.volume import netapp_nas
from mox import IgnoreArg
import random
import suds


class FakeObject(object):
    pass


class FakeRequest(object):
    def __init__(self, name=None, args=None):
        self.Name = name
        self.Args = args


class FakeStartResp(object):
    def __init__(self):
        self.Tag = random.randint(1, 100)
        self.Records = random.randint(1, 10)


class FakeStatus(object):
    def __init__(self, status):
        self.Status = status


class FakeAggregates(object):
    def __init__(self, max_aggr_id):
        class AggrSizeAvail(object):
            def __init__(self, filer_id, avail):
                self.AggregateSize = FakeObject()
                self.FilerId = filer_id
                self.AggregateName = 'filer%d:aggr0' % filer_id
                setattr(self.AggregateSize, 'SizeAvailable', avail)

        class AggregateInfo(object):
            def __init__(self):
                self.AggregateInfo = [AggrSizeAvail(1, 10),
                                      AggrSizeAvail(2, 20),
                                      AggrSizeAvail(3, 1),
                                      AggrSizeAvail(max_aggr_id, 50),
                                      AggrSizeAvail(5, 15)]

        self.Aggregates = AggregateInfo()


class FakeNfsRules(object):
    def __init__(self):
        class Rules(object):
            def __init__(self):
                self.rules = [
                    {'exports-rule-info-2': [
                        {'security-rules': [
                            {'security-rule-info': [
                                {'root': [
                                    {'exports-hostname-info': [
                                        {'name': 'allowed_host'},
                                        {'name': 'disallowed_host'}]}
                                ]}
                            ]}
                        ]}
                    ]}
                ]

        self.Results = Rules()


class FakeHost(object):
    def __init__(self, id):
        self.HostId = id


class FakeHostInfo(object):
    def __init__(self):
        self.Hosts = FakeObject()
        setattr(self.Hosts, 'HostInfo', [FakeHost(1), FakeHost(2)])


class FakeVolumeInfo(object):
    def __init__(self):
        self.Volumes = FakeObject()
        setattr(self.Volumes, 'VolumeInfo', 'info')


class FakeVolume(object):
    def __init__(self, name, host_id, snap_id=0):
        self.VolumeName = name
        self.HostId = host_id
        self.SnapshotId = snap_id


class FakeFilter(object):
    def __init__(self, id=0):
        self.ObjectNameOrId = id


class FakeSnapsInfo(object):
    def __init__(self):
        self.Snapshots = FakeObject()
        setattr(self.Snapshots, 'SnapshotInfo', 'info')


class FakeTimestamp(object):
    def __init__(self, monitor_name='file_system', last_stamp=1):
        self.MonitorName = monitor_name
        self.LastMonitoringTimestamp = last_stamp


class FakeMonitorTimestampList(object):
    def __init__(self):
        self.DfmMonitoringTimestamp = [FakeTimestamp()]


class FakeSnapshotList(object):
    def __init__(self, busy_snapshot=None):
        self.Results = FakeObject()
        snap_info = [{'snapshot-info':
                           [{'name': ['not_busy_snapshot'],
                             'busy': ['false']},
                            {'name': ['another_not_busy'],
                             'busy': ['false']}]
                     }]

        if busy_snapshot is not None:
            snap_info[0]['snapshot-info'].append({'name': [busy_snapshot],
                                                  'busy': ['true']})

        setattr(self.Results, 'snapshots', snap_info)


class NetAppNASDriverTestCase(test.TestCase):
    """
    Tests Netapp-specific NAS driver
    """

    def setUp(self):
        super(NetAppNASDriverTestCase, self).setUp()

        self._context = context.get_admin_context()
        self._driver = netapp_nas.NetAppNASDriver()
        self._driver._client = self.mox.CreateMock(netapp_nas.NetAppApiClient)
        self._driver._helpers = {'CIFS': self.mox.CreateMock(
                                                netapp_nas.NetAppCIFSHelper),
                                 'NFS': self.mox.CreateMock(
                                                netapp_nas.NetAppNFSHelper)}
        self._driver.db = self.mox.CreateMockAnything()

    def tearDown(self):
        super(NetAppNASDriverTestCase, self).tearDown()

    def test_setup_check(self):
        drv = self._driver
        client = drv._client
        client.check_configuration()
        self.mox.ReplayAll()
        drv.check_for_setup_error()

    def test_load_balancer(self):
        drv = self._driver
        max_aggr_id = 123

        drv._client.get_available_aggregates().AndReturn(
                                                FakeAggregates(max_aggr_id))

        self.mox.ReplayAll()

        aggr = drv._find_best_aggregate()

        self.assertEquals(max_aggr_id, aggr.FilerId)

    def test_create_volume(self):
        drv = self._driver
        client = drv._client
        volume = {'id': 'vol_id', 'size': 1}
        max_aggr_id = 123

        client.get_available_aggregates().AndReturn(
                                                FakeAggregates(max_aggr_id))
        client.send_request_to(max_aggr_id, 'volume-create', IgnoreArg())

        self.mox.ReplayAll()

        drv.create_volume(volume)

        self.assertEqual(max_aggr_id, drv._share_table[volume['id']])

    def test_delete_volume_target_exists(self):
        drv = self._driver
        client = drv._client
        vol_id = 'volume-vol_id'
        volume = {'id': vol_id, 'size': 1}
        max_aggr_id = 123

        client.get_available_aggregates().\
                            AndReturn(FakeAggregates(max_aggr_id))
        client.send_request_to(max_aggr_id, 'volume-create', IgnoreArg())
        client.send_request_to(max_aggr_id, 'volume-offline', IgnoreArg())
        client.send_request_to(max_aggr_id, 'volume-destroy', IgnoreArg())

        self.mox.ReplayAll()

        drv.create_volume(volume)
        drv.delete_volume(volume)

        self.assertEquals(len(drv._share_table.keys()), 0)

    def test_share_create(self):
        drv = self._driver
        ctx = self._context
        protocol = 'CIFS'
        share = {'proto': protocol}
        volume = {'size': 1, 'id': '1234-abcd-5678'}

        drv._helpers[protocol].create_share(IgnoreArg(), share, volume)

        self.mox.ReplayAll()

        drv.create_share(ctx, share, volume)

    def test_share_delete(self):
        drv = self._driver
        ctx = self._context
        protocol = 'NFS'
        helper = drv._helpers[protocol]
        ip = '172.10.0.1'
        export = '/export_path'
        share = {'proto': protocol, 'export_location': ':'.join([ip, export])}
        volume = {'id': 'abcd-1234'}
        fake_access_rules = [1, 2, 3]

        helper.get_target(share).AndReturn(ip)
        access_rules = drv.db.share_access_get_all_for_share(IgnoreArg(),
                                                             IgnoreArg()).\
                            AndReturn(fake_access_rules)
        for rule in access_rules:
            helper.access_deny(ctx, share, volume, rule)
        helper.delete_share(share, IgnoreArg())

        self.mox.ReplayAll()

        drv.delete_share(ctx, share, volume)

    def test_access_allow(self):
        drv = self._driver
        proto = 'CIFS'
        ctx = self._context
        share = {'proto': proto}
        volume = {}
        access = {}

        drv._helpers[proto].access_allow(ctx, share, volume, access)

        self.mox.ReplayAll()

        drv.access_allow(ctx, share, volume, access)

    def test_access_deny(self):
        drv = self._driver
        proto = 'CIFS'
        ctx = self._context
        share = {'proto': proto}
        volume = {}
        access = {}

        drv._helpers[proto].access_deny(ctx, share, volume, access)

        self.mox.ReplayAll()

        drv.access_deny(ctx, share, volume, access)

    def test_create_snapshot(self):
        drv = self._driver
        client = drv._client
        vol_id = 'abcd-1234'
        snapshot = {'volume_id': vol_id, 'name': 'snap_name'}

        client.send_request_to(IgnoreArg(), 'snapshot-create', IgnoreArg())

        self.mox.ReplayAll()

        drv.create_snapshot(snapshot)

    def test_delete_snapshot(self):
        drv = self._driver
        client = drv._client
        snapshot = {'volume_id': 'abcd-1234', 'name': 'snap_name'}

        client.send_request_to(IgnoreArg(),
                               'snapshot-list-info',
                               IgnoreArg(),
                               do_response_check=False).\
                                 AndReturn(FakeSnapshotList())
        client.send_request_to(IgnoreArg(), 'snapshot-delete', IgnoreArg())

        self.mox.ReplayAll()

        drv.delete_snapshot(snapshot)

    def test_delete_busy_snapshot(self):
        drv = self._driver
        client = drv._client
        busy_snap_name = 'busy_snapshot'
        snapshot = {'volume_id': 'abcd-1234', 'name': busy_snap_name}

        client.send_request_to(IgnoreArg(),
                               'snapshot-list-info',
                               IgnoreArg(),
                               do_response_check=False).\
                                   AndReturn(FakeSnapshotList(busy_snap_name))

        self.mox.ReplayAll()

        self.assertRaises(exception.SnapshotIsBusy, drv.delete_snapshot,
                          snapshot)

    def test_create_volume_from_snapshot(self):
        drv = self._driver
        client = drv._client
        snapshot = {'id': 'abcd-21345', 'name': 'some_name', 'volume_id': 'id'}
        volume = {'id': 'abcd-1235'}

        client.send_request_to(IgnoreArg(), 'volume-clone-create', IgnoreArg())

        self.mox.ReplayAll()

        drv.create_volume_from_snapshot(volume, snapshot)

    def test_no_aggregates_available(self):
        drv = self._driver
        volume = None

        drv._client.get_available_aggregates().AndReturn(None)

        self.mox.ReplayAll()

        self.assertRaises(exception.Error, drv.create_volume, volume)


class NetAppNfsHelperTestCase(test.TestCase):
    """
    Tests Netapp-specific NFS driver
    """
    def setUp(self):
        super(NetAppNfsHelperTestCase, self).setUp()

        fake_client = self.mox.CreateMock(netapp_nas.NetAppApiClient)
        self._driver = netapp_nas.NetAppNFSHelper(fake_client)

    def tearDown(self):
        super(NetAppNfsHelperTestCase, self).tearDown()

    def test_create_share(self):
        drv = self._driver
        client = drv._client
        target = 123
        volume = {'id': 'abc-1234-567'}
        share = None

        client.send_request_to(target, 'nfs-exportfs-append-rules-2',
                               IgnoreArg())
        client.get_host_ip_by(target).AndReturn('host:export')

        self.mox.ReplayAll()

        export = drv.create_share(target, share, volume)

        self.assertEquals(export.find('-'), -1)

    def test_delete_share(self):
        drv = self._driver
        client = drv._client
        share = {'export_location': 'host:export'}
        volume = None

        client.send_request_to(IgnoreArg(), 'nfs-exportfs-delete-rules',
                               IgnoreArg())

        self.mox.ReplayAll()

        drv.delete_share(share, volume)

    def test_invalid_access_allow(self):
        drv = self._driver
        share = None
        volume = None
        access = {'access_type': 'passwd'}  # passwd type is not supported

        self.assertRaises(exception.Error, drv.access_allow, context, share,
                          volume, access)

    def test_access_allow(self):
        drv = self._driver
        client = drv._client
        share = {'export_location': 'host:export'}
        volume = None
        access = {'access_to': ['127.0.0.1', '127.0.0.2'],
                  'access_type': 'ip'}

        client.send_request_to(IgnoreArg(), 'nfs-exportfs-list-rules-2',
                               IgnoreArg()).AndReturn(FakeNfsRules())
        client.send_request_to(IgnoreArg(), 'nfs-exportfs-append-rules-2',
                               IgnoreArg())

        self.mox.ReplayAll()

        drv.access_allow(context, share, volume, access)

    def test_access_deny(self):
        drv = self._driver
        client = drv._client
        share = {'export_location': 'host:export'}
        volume = None
        access = {'access_to': ['127.0.0.1', '127.0.0.2']}

        client.send_request_to(IgnoreArg(), 'nfs-exportfs-list-rules-2',
                               IgnoreArg()).AndReturn(FakeNfsRules())
        client.send_request_to(IgnoreArg(), 'nfs-exportfs-append-rules-2',
                               IgnoreArg())

        self.mox.ReplayAll()

        drv.access_deny(context, share, volume, access)

    def test_get_target(self):
        drv = self._driver
        ip = '172.18.0.1'
        export_path = '/home'
        share = {'export_location': ':'.join([ip, export_path])}

        self.assertEquals(drv.get_target(share), ip)


class NetAppCifsHelperTestCase(test.TestCase):
    """
    Tests Netapp-specific CIFS driver
    """
    def setUp(self):
        super(NetAppCifsHelperTestCase, self).setUp()

        fake_client = self.mox.CreateMock(netapp_nas.NetAppApiClient)
        self._driver = netapp_nas.NetAppCIFSHelper(fake_client)

    def tearDown(self):
        super(NetAppCifsHelperTestCase, self).tearDown()

    def test_create_share(self):
        drv = self._driver
        client = drv._client
        target = 123
        volume = {'id': 'abc-1234-567'}
        share = None
        ip = '172.0.0.1'

        client.send_request_to(target, 'cifs-status').AndReturn(
                                                        FakeStatus('stopped'))
        client.send_request_to(target, 'cifs-start',
                               do_response_check=False)
        client.send_request_to(target, 'system-cli', IgnoreArg())
        client.send_request_to(target, 'cifs-share-add', IgnoreArg())
        client.send_request_to(target, 'cifs-share-ace-delete', IgnoreArg())
        client.get_host_ip_by(target).AndReturn(ip)

        self.mox.ReplayAll()

        export = drv.create_share(target, share, volume)

        self.assertEquals(export.find('-'), -1)
        self.assertTrue(export.startswith('//' + ip))

    def test_delete_share(self):
        drv = self._driver
        client = drv._client
        volume = None
        ip = '172.10.0.1'
        export = 'home'
        share = {'export_location': '//%s/%s' % (ip, export)}

        client.send_request_to(IgnoreArg(), 'cifs-share-delete', IgnoreArg())

        self.mox.ReplayAll()

        drv.delete_share(share, volume)

    def test_allow_access_by_ip(self):
        drv = self._driver
        access = {'access_type': 'ip', 'access_to': '123.123.123.123'}
        share = None
        volume = None

        self.assertRaises(exception.Error, drv.access_allow, context, share,
                          volume, access)

    def test_allow_access_by_passwd_invalid_user(self):
        drv = self._driver
        client = drv._client
        access = {'access_type': 'passwd', 'access_to': 'user:pass'}
        ip = '172.0.0.1'
        export = 'export_path'
        share = {'export_location': '//%s/%s' % (ip, export)}
        volume = None

        client.send_request_to(ip, 'useradmin-user-list', IgnoreArg(),
                               do_response_check=False).\
                                    AndReturn(FakeStatus('failed'))

        self.mox.ReplayAll()

        self.assertRaises(exception.Error, drv.access_allow, context, share,
                          volume, access)

    def test_allow_access_by_passwd_existing_user(self):
        drv = self._driver
        client = drv._client
        access = {'access_type': 'passwd', 'access_to': 'user:pass'}
        ip = '172.0.0.1'
        export = 'export_path'
        share = {'export_location': '//%s/%s' % (ip, export)}
        volume = None

        client.send_request_to(ip, 'useradmin-user-list', IgnoreArg(),
                               do_response_check=False).\
                                    AndReturn(FakeStatus('passed'))
        client.send_request_to(ip, 'cifs-share-ace-set', IgnoreArg())

        self.mox.ReplayAll()

        drv.access_allow(context, share, volume, access)

    def test_deny_access(self):
        drv = self._driver
        client = drv._client
        access = {'access_type': 'passwd', 'access_to': 'user:pass'}
        ip = '172.0.0.1'
        export = 'export_path'
        share = {'export_location': '//%s/%s' % (ip, export)}
        volume = None

        client.send_request_to(ip, 'cifs-share-ace-delete', IgnoreArg())

        self.mox.ReplayAll()

        drv.access_deny(context, share, volume, access)

    def test_get_target(self):
        drv = self._driver
        ip = '172.10.0.1'
        export = 'export_path'
        share = {'export_location': '//%s/%s' % (ip, export)}

        self.assertEquals(drv.get_target(share), ip)


class NetAppNASHelperTestCase(test.TestCase):
    def setUp(self):
        super(NetAppNASHelperTestCase, self).setUp()

        fake_client = self.mox.CreateMock(suds.client.Client)
        self._driver = netapp_nas.NetAppNASHelperBase(fake_client)

    def tearDown(self):
        super(NetAppNASHelperTestCase, self).tearDown()

    def test_create_share(self):
        drv = self._driver
        target_id = None
        share = None
        volume = None
        self.assertRaises(NotImplementedError, drv.create_share, target_id,
                          share, volume)

    def test_delete_share(self):
        drv = self._driver
        share = None
        volume = None
        self.assertRaises(NotImplementedError, drv.delete_share, share, volume)

    def test_access_allow(self):
        drv = self._driver
        share = None
        volume = None
        ctx = None
        access = None
        self.assertRaises(NotImplementedError, drv.access_allow,
                          ctx, share, volume, access)

    def test_access_deny(self):
        drv = self._driver
        share = None
        volume = None
        ctx = None
        access = None
        self.assertRaises(NotImplementedError, drv.access_deny,
                          ctx, share, volume, access)

    def test_get_target(self):
        drv = self._driver
        share = None
        self.assertRaises(NotImplementedError, drv.get_target, share)


class NetAppApiClientTestCase(test.TestCase):
    """
    Tests for NetApp DFM API client
    """
    def setUp(self):
        super(NetAppApiClientTestCase, self).setUp()
        self._driver = netapp_nas.NetAppApiClient()
        self._context = context.get_admin_context()

        self._driver._client = self.mox.CreateMock(suds.client.Client)
        self._driver._client.factory = self.mox.CreateMock(suds.client.Factory)
        # service object is generated dynamically from XML
        self._driver._client.service = self.mox.CreateMockAnything(
                                                suds.client.ServiceSelector)

    def tearDown(self):
        super(NetAppApiClientTestCase, self).tearDown()

    def test_get_host_by_ip(self):
        drv = self._driver
        client = drv._client
        service = client.service
        host_id = 123

        # can't use 'filter' because it's predefined in Python
        fltr = client.factory.create('HostListInfoIterStart').\
                                        AndReturn(FakeFilter())

        resp = service.HostListInfoIterStart(HostListInfoIterStart=fltr).\
                                        AndReturn(FakeStartResp())
        service.HostListInfoIterNext(Tag=resp.Tag, Maximum=resp.Records).\
                                        AndReturn(FakeHostInfo())
        service.HostListInfoIterEnd(Tag=resp.Tag)

        self.mox.ReplayAll()

        drv.get_host_ip_by(host_id)

    def test_get_available_aggregates(self):
        drv = self._driver
        client = drv._client
        service = client.service

        resp = service.AggregateListInfoIterStart().AndReturn(FakeStartResp())
        service.AggregateListInfoIterNext(Tag=resp.Tag, Maximum=resp.Records)
        service.AggregateListInfoIterEnd(resp.Tag)

        self.mox.ReplayAll()

        drv.get_available_aggregates()

    def test_send_successfull_request(self):
        drv = self._driver
        client = drv._client
        service = client.service
        factory = client.factory

        target = 1
        args = '<xml></xml>'
        responce_check = False
        request = factory.create('Request').AndReturn(FakeRequest())

        service.ApiProxy(Target=target, Request=request)

        self.mox.ReplayAll()

        drv.send_request_to(target, request, args, responce_check)

    def test_send_failing_request(self):
        drv = self._driver
        client = drv._client
        service = client.service
        factory = client.factory

        target = 1
        args = '<xml></xml>'
        responce_check = True
        request = factory.create('Request').AndReturn(FakeRequest())

        service.ApiProxy(Target=target, Request=request).AndRaise(
                                        exception.Error())

        self.mox.ReplayAll()

        self.assertRaises(exception.Error, drv.send_request_to,
                          target, request, args, responce_check)

    def test_successfull_setup(self):
        drv = self._driver
        for flag in drv.REQUIRED_FLAGS:
            setattr(netapp_nas.FLAGS, flag, 'val')

        drv.check_configuration()

    def test_failing_setup(self):
        drv = self._driver
        self.assertRaises(exception.Error, drv.check_configuration)
