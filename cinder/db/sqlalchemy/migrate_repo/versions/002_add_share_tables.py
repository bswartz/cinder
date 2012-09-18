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

from sqlalchemy import MetaData, Table, String, DateTime, Boolean
from sqlalchemy import Integer, Column, ForeignKey
from cinder.openstack.common import log as logging

LOG = logging.getLogger(__name__)


def upgrade(migrate_engine):
    """Convert volume and snapshot id columns from int to varchar."""
    meta = MetaData()
    meta.bind = migrate_engine

    volumes = Table('volumes', meta, autoload=True)

    shares = Table('shares', meta,
                               Column('created_at', DateTime),
                               Column('updated_at', DateTime),
                               Column('deleted_at', DateTime),
                               Column('deleted', Boolean),
                               Column('id', Integer(),
                                      primary_key=True, nullable=False),
                               Column('proto', String(255)),
                               Column('volume_id', String(36),
                                      ForeignKey('volumes.id')),
                               Column('export_location', String(255)))

    access_map = Table('share_access_map', meta,
                       Column('created_at', DateTime),
                       Column('updated_at', DateTime),
                       Column('deleted_at', DateTime),
                       Column('deleted', Boolean),
                       Column('id', Integer(),
                              primary_key=True, nullable=False),
                       Column('volume_id', String(36)),
                       Column('access_type', String(255)),
                       Column('access_to', String(255)),
                       Column('state', String(255)))

    shares.create()
    access_map.create()


def downgrade(migrate_engine):
    """Convert volume and snapshot id columns back to int."""
    meta = MetaData()
    meta.bind = migrate_engine
    shares = Table('shares', meta, autoload=True)
    access_map = Table('share_access_map', meta, autoload=True)
    shares.drop()
    access_map.drop()
