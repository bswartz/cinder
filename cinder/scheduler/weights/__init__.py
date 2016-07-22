# Copyright (c) 2011 OpenStack Foundation.
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
Scheduler host weights
"""

import random

from cinder.scheduler import base_weight


class WeighedHost(base_weight.WeighedObject):
    def to_dict(self):
        return {
            'weight': self.weight,
            'host': self.obj.host,
        }

    def __repr__(self):
        return ("WeighedHost [host: %s, weight: %s]" %
                (self.obj.host, self.weight))


class BaseHostWeigher(base_weight.BaseWeigher):
    """Base class for host weights."""
    pass


class TopHostWeightHandler(base_weight.BaseWeightHandler):
    object_class = WeighedHost

    def __init__(self, namespace):
        super(TopHostWeightHandler, self).__init__(BaseHostWeigher, namespace)

    def get_weighed_objects(self, weigher_classes, obj_list,
                            weighing_properties):
        """Return a sorted (descending) list of WeighedHosts."""
        weighed_objs = base_weight.BaseWeightHandler.get_weighed_objects(
            self, weigher_classes, obj_list, weighing_properties)
        return sorted(weighed_objs, key=lambda x: x.weight, reverse=True)


class StochasticHostWeightHandler(base_weight.BaseWeightHandler):
    object_class = WeighedHost

    def __init__(self, namespace):
        super(StochasticHostWeightHandler, self).__init__(BaseHostWeigher,
                                                          namespace)

    def get_weighed_objects(self, weigher_classes, obj_list,
                            weighing_properties):
        weighed_objs = base_weight.BaseWeightHandler.get_weighed_objects(
            self, weigher_classes, obj_list, weighing_properties)

        # First compute the total weight of all the objects and the upper
        # bound for each object to "win" the lottery.
        total_weight = 0
        table = []
        for weighed_obj in weighed_objs:
            total_weight += weighed_obj.weight
            max_value = total_weight
            table.append((max_value, weighed_obj))
        # Now draw a random value with the computed range
        winning_value = random.random() * total_weight
        # Scan the table to find the first object with a maximum higher than
        # the random number. This is the winner. Save the index.
        winning_index = 0
        for (max_value, weighed_obj) in table:
            if max_value > winning_value:
                winning_index = weighed_objs.index(weighed_obj)
        print 'Winner %f %d' % (winning_value, winning_index)
        # Rotate the array so the winner is first
        return weighed_objs[winning_index:] + weighed_objs[0:winning_index]
