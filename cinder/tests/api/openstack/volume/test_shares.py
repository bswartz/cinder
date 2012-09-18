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

import webob

from cinder.api.openstack.volume import shares
from cinder import test
from cinder.tests.api.openstack import fakes
from cinder.volume import api as volume_api


class ShareApiTest(test.TestCase):
    """Share Api Test"""
    def setUp(self):
        super(ShareApiTest, self).setUp()
        self.controller = shares.ShareController()

        self.stubs.Set(volume_api.API, 'get_all_shares_volumes',
                                        fakes.stub_get_all_shares_volumes)
        self.stubs.Set(volume_api.API, 'get_share_volume',
            fakes.stub_share_volume_get)
        self.stubs.Set(volume_api.API, 'delete', fakes.stub_volume_delete)
        self.stubs.Set(volume_api.API, 'create_share',
                                        fakes.stub_share_create)

    def test_share_show(self):
        req = fakes.HTTPRequest.blank('/v1/shares/1')
        res_dict = self.controller.show(req, '1')
        expected = {'share': {'display_name': 'displayname',
                               'export_location': 'fake_location',
                               'id': 'fake_volume_id',
                               'share_type': 'fakeproto',
                               'size': 1,
                               'status': 'fakestatus'}}
        self.assertEqual(res_dict, expected)

    def test_share_show_not_found(self):
        self.stubs.Set(volume_api.API, 'get_share_volume',
            fakes.stub_share_get_notfound)
        req = fakes.HTTPRequest.blank('/v1/shares/1')
        self.assertRaises(webob.exc.HTTPNotFound,
            self.controller.show,
            req, '1')

    def test_share_delete(self):
        req = fakes.HTTPRequest.blank('/v1/shares/1')
        resp = self.controller.delete(req, 1)
        self.assertEqual(resp.status_int, 202)

    def test_share_delete_no_share(self):
        self.stubs.Set(volume_api.API, 'get_share_volume',
            fakes.stub_share_get_notfound)
        req = fakes.HTTPRequest.blank('/v1/shares/1')
        self.assertRaises(webob.exc.HTTPNotFound,
            self.controller.delete,
            req,
            1)

    def test_share_list(self):
        req = fakes.HTTPRequest.blank('/v1/shares')
        res_dict = self.controller.index(req)
        expected = {'shares': [{'display_name': 'displayname',
                              'export_location': 'fake_location',
                              'id': '1',
                              'share_type': 'fakeproto',
                              'size': 1,
                              'status': 'fakestatus'}]}
        self.assertEqual(res_dict, expected)

    def test_share_create(self):
        req = fakes.HTTPRequest.blank('/v1/shares')
        share = {'size': 1,
                 'proto': 'fakeproto',
                 'display_name': 'fakename'}
        body = {'share': share}
        res_dict = self.controller.create(req, body)
        expected = {'share': {'display_name': 'fakename',
                              'export_location': 'fake_location',
                              'id': '1',
                              'share_type': 'fakeproto',
                              'size': 1,
                              'status': 'fakestatus'}}
        self.assertEqual(res_dict, expected)

    def test_share_create_no_body(self):
        body = {}
        req = fakes.HTTPRequest.blank('/v1/shares')
        self.assertRaises(webob.exc.HTTPUnprocessableEntity,
            self.controller.create,
            req,
            body)
