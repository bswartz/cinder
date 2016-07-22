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
Tests For Filter Scheduler.
"""

import ddt
import mock
from oslo_config import cfg
import random

from cinder import test
from cinder.scheduler import base_weight
from cinder.scheduler.weights import StochasticHostWeightHandler
from cinder.tests.unit.scheduler import fakes


CONF = cfg.CONF


@ddt.ddt
class StochasticWeightHandlerTestCase(test.TestCase):
    """Test case for StochasticHostWeightHandler."""

    def setUp(self):
        super(StochasticWeightHandlerTestCase, self).setUp()

    @ddt.data(
        (0.0, 'A'),
        (0.1, 'A'),
        (0.2, 'B'),
        (0.3, 'B'),
        (0.4, 'B'),
        (0.5, 'B'),
        (0.6, 'B'),
        (0.7, 'C'),
        (0.8, 'C'),
        (0.9, 'C'),
    )
    @ddt.unpack
    def test_get_weighed_objects_correct(self, rand_value, expected_obj):
        self.mock_object(random,
                         'random',
                         mock.Mock(return_value=rand_value))

        class MapWeigher(base_weight.BaseWeigher):
            minval = 0
            maxval = 100

            def _weigh_object(self, obj, weight_map):
                return weight_map[obj]

        weight_map = { 'A': 1, 'B': 3, 'C': 2 }
        objs = weight_map.keys()

        weigher_classes = [ MapWeigher ]
        handler = StochasticHostWeightHandler('fake_namespace')
        weighted_objs = handler.get_weighed_objects(weigher_classes,
                                                    objs,
                                                    weight_map)
        winner = weighted_objs[0].obj
        self.assertEqual(expected_obj, winner)

    """def test__choose_top_host_normal(self):
        sched = fakes.FakeFilterScheduler()
        # Mock out stuff we don't want to test
        self.mock_object(host_manager.HostState, 'consume_from_volume')
        request_spec = {'volume_properties': ''}
        # Call the method with default options
        top_host = sched._choose_top_host(fakes.FAKE_WEIGHTED_HOSTS,
                                          request_spec)
        self.assertEqual('host1', top_host.obj.host)

    def test__choose_top_host_stochastic(self):
        sched = fakes.FakeFilterScheduler()
        # Mock out stuff we don't want to test
        self.mock_object(host_manager.HostState, 'consume_from_volume')
        request_spec = {'volume_properties': ''}
        # Set the config option to true
        CONF.set_override('filter_schedule_stochastic', True)
        # Force a high random number so the last host should be chosen
        self.mock_object(random,
                         'random',
                         mock.Mock(return_value=0.99))
        top_host = sched._choose_top_host(fakes.FAKE_WEIGHTED_HOSTS,
                                          request_spec)
        self.assertEqual('host3', top_host.obj.host)"""
