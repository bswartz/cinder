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

"""NFS client utilities."""

import hashlib
import os

from oslo.config import cfg

from cinder import exception
from cinder.openstack.common.gettextutils import _
from cinder.openstack.common import log as logging
from cinder.openstack.common import processutils as putils
from cinder.volume.configuration import Configuration

LOG = logging.getLogger(__name__)

nfs_client_opts = [
    cfg.StrOpt('nfs_mount_point_base',
               default='$state_path/mnt',
               help='Base dir containing mount points for nfs shares'),
    cfg.StrOpt('nfs_mount_options',
               default=None,
               help='Mount options passed to the nfs client. See section '
                    'of the nfs man page for details'),
]


CONF = cfg.CONF
CONF.register_opts(nfs_client_opts)


class NfsClient(object):

    def __init__(self, execute=putils.execute, root_helper="sudo",
                 *args, **kwargs):
        self.configuration = kwargs.get('configuration', None)
        if self.configuration:
            self.configuration.append_config_values(nfs_client_opts)
        else:
            self.configuration = Configuration(nfs_client_opts)
        self.root_helper = root_helper
        self._set_execute(execute)

    def _set_execute(self, execute):
        self._execute = execute

    def _get_hash_str(self, base_str):
        """returns string that represents hash of base_str
        (in a hex format).
        """
        return hashlib.md5(base_str).hexdigest()

    def get_mount_point(self, nfs_share):
        """
        :param nfs_share: example 172.18.194.100:/var/nfs
        """
        return os.path.join(self.configuration.nfs_mount_point_base,
                            self._get_hash_str(nfs_share))

    def _read_mounts(self):
        (out, err) = self._execute('mount', check_exit_code=0)
        lines = out.split('\n')
        mounts = {}
        for line in lines:
            tokens = line.split()
            if 2 < len(tokens):
                device = tokens[0]
                mnt_point = tokens[2]
                mounts[mnt_point] = device
        return mounts

    def mount(self, nfs_share, flags=None):
        """Mount NFS share."""
        mount_path = self.get_mount_point(nfs_share)

        if mount_path in self._read_mounts():
            LOG.info(_('Already mounted: %s') % mount_path)
            return

        self._execute('mkdir', '-p', mount_path, check_exit_code=0)

        mnt_cmd = ['mount', '-t', 'nfs']
        if self.configuration.nfs_mount_options is not None:
            mnt_cmd.extend(['-o', self.configuration.nfs_mount_options])
        if flags is not None:
            mnt_cmd.extend(flags)
        mnt_cmd.extend([nfs_share, mount_path])

        self._execute(*mnt_cmd, root_helper=self.root_helper,
                      run_as_root=True, check_exit_code=0)
