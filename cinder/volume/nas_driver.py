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
Drivers for shares.

"""

import os
import re
import ConfigParser

from cinder import exception
from cinder import flags
from cinder.openstack.common import log as logging
from cinder.openstack.common import cfg
from cinder.volume import driver


LOG = logging.getLogger(__name__)

share_opts = [
    cfg.StrOpt('share_export_root',
               default='$state_path/mnt',
               help='Base folder where exported shares are located'),
    cfg.StrOpt('share_export_ip',
               default=None,
               help='IP to be added to export string'),
    cfg.StrOpt('smb_config_path',
               default='$state_path/smb.conf')
]

FLAGS = flags.FLAGS
FLAGS.register_opts(share_opts)


class NasDriverMixin(object):
    """Class defines interface to extend volume driver with NAS related
       functionality"""

    def __init__(self, *args, **kwargs):
        super(NasDriverMixin, self).__init__(*args, **kwargs)

    def create_share(self, share, volume):
        """Is called after create volume to create share on the volume"""
        raise NotImplementedError()

    def delete_share(self, share, volume):
        """Is called after create volume to create share on the volume"""
        raise NotImplementedError()

    def ensure_share(self, share, volume):
        """Is called after create volume to create share on the volume"""
        raise NotImplementedError()

    def access_allow(self, context, share, volume, access):
        """Allow access to the share"""
        raise NotImplementedError()

    def access_deny(self, context, share, volume, access):
        """Deny access to the share"""
        raise NotImplementedError()


class ShareDriver(NasDriverMixin, driver.VolumeDriver):
    """Executes commands relating to Shares."""

    def __init__(self, *args, **kwargs):
        """Do initialization"""
        super(ShareDriver, self).__init__(*args, **kwargs)
        self._helpers = {
            'CIFS': CIFSHelper(self._execute, FLAGS.smb_config_path),
            'NFS': NFSHelper(self._execute),
            }

    def do_setup(self, context):
        """Any initialization the volume driver does while starting"""
        super(ShareDriver, self).do_setup(context)
        for helper in self._helpers.values():
            helper.init()

    def create_volume(self, volume):
        """Create LVM volume that will be represented as NAS share"""
        super(ShareDriver, self).create_volume(volume)
        #create file system
        device_name = self.local_path(volume)
        self._execute('mkfs.ext4', device_name, run_as_root=True)

    def ensure_export(self, ctx, volume):
        """Synchronously recreates an export for a logical volume."""
        device_name = self.local_path(volume)
        self._mount_device(volume, device_name)

    def create_export(self, ctx, volume):
        """Exports the volume. Can optionally return a Dictionary of changes
        to the volume object to be persisted."""
        device_name = self.local_path(volume)
        location = self._mount_device(volume, device_name)
        return {'provider_location': location}

    def remove_export(self, ctx, volume):
        """Removes an access rules for a logical volume."""
        mount_path = self._get_mount_path(volume)
        if os.path.exists(mount_path):
            #umount, may be busy
            try:
                self._execute('umount', '-f', mount_path, run_as_root=True)
            except exception.ProcessExecutionError, exc:
                if 'device is busy' in exc.message:
                    raise exception.VolumeIsBusy(volume_name=volume['name'])
                else:
                    LOG.info('Unable to umount: ', exc)
            #remove dir
            try:
                os.rmdir(mount_path)
            except OSError:
                LOG.info('Unable to delete ', mount_path)

    def check_for_export(self, ctx, volume_id):
        """Make sure volume is exported."""
        pass

    def create_share(self, ctx, share, volume):
        """Is called after create volume to create share on the volume"""
        location = self._get_mount_path(volume)
        location = self._get_helper(share).create_export(location,
                                                         volume['name'])
        return location

    def ensure_share(self, ctx, share, volume):
        location = self._get_mount_path(volume)
        self._get_helper(share).create_export(location, volume['name'],
                                              recreate=True)
        rules = self.db.share_access_get_all_for_share(ctx, volume['id'])
        for rule in rules:
            if rule['state'] == rule.STATE_ACTIVE:
                try:
                    self.access_allow(ctx, share, volume, rule)
                except exception.ShareAccessExists:
                    pass

    def delete_share(self, ctx, share, volume):
        """Delete a share"""
        rules = self.db.share_access_get_all_for_share(ctx, volume['id'])
        for rule in rules:
            try:
                    self.access_deny(ctx, share, volume, rule)
            except exception.ProcessExecutionError:
                LOG.info("Can't remove access rule %r" % rule.id)
        #remove share
        try:
            location = self._get_mount_path(volume)
            self._get_helper(share).remove_export(location, volume['name'])
        except exception.ProcessExecutionError:
            LOG.info("Can't remove share %r" % share['id'])
        except exception.InvalidShare, exc:
            LOG.info(exc.message)

    def access_allow(self, ctx, share, volume, access):
        """Allow access to the share"""
        location = self._get_mount_path(volume)
        self._get_helper(share).access_allow(location, volume['name'],
                                             access['access_type'],
                                             access['access_to'])

    def access_deny(self, ctx, share, volume, access):
        """Allow access to the share"""
        location = self._get_mount_path(volume)
        self._get_helper(share).access_deny(location, volume['name'],
                                             access['access_type'],
                                             access['access_to'])

    def _get_helper(self, share):
        if share['proto'].startswith('NFS'):
            return self._helpers['NFS']
        elif share['proto'].startswith('CIFS'):
            return self._helpers['CIFS']
        else:
            raise exception.InvalidShare(reason='Wrong share type')

    def _mount_device(self, volume, device_name):
        """Mount LVM volume and ignore if already mounted"""
        mount_path = self._get_mount_path(volume)
        self._execute('mkdir', '-p', mount_path)
        try:
            self._execute('mount', device_name, mount_path,
                          run_as_root=True, check_exit_code=True)
            self._execute('chmod', '777', mount_path,
                          run_as_root=True, check_exit_code=True)
        except exception.ProcessExecutionError as exc:
            if 'already mounted' in exc.stderr:
                LOG.warn(_("%s is already mounted"), device_name)
            else:
                raise
        return mount_path

    def _get_mount_path(self, volume):
        """Returns path where share is mounted"""
        return os.path.join(FLAGS.share_export_root, volume['name'])


class NASHelperBase(object):
    """Interface to work with share"""

    def __init__(self, execute):
        self._execute = execute

    def init(self):
        pass

    def create_export(self, local_path, share_name, recreate=False):
        """Create new export, delete old one if exists"""
        raise NotImplementedError()

    def remove_export(self, local_path, share_name):
        """Remove export"""
        raise NotImplementedError()

    def access_allow(self, local_path, share_name, access_type, access):
        """Allow access to the host"""
        raise NotImplementedError()

    def access_deny(self, local_path, share_name, access_type, access,
                    force=False):
        """Deny access to the host"""
        raise NotImplementedError()


class NFSHelper(NASHelperBase):
    """Interface to work with share"""

    def __init__(self, execute):
        super(NFSHelper, self).__init__(execute)
        try:
            self._execute('exportfs', check_exit_code=True,
                          run_as_root=True)
        except exception.ProcessExecutionError:
            raise exception.Error('NFS server not found')

    def create_export(self, local_path, share_name, recreate=False):
        """Create new export, delete old one if exists"""
        return ':'.join([FLAGS.share_export_ip, local_path])

    def remove_export(self, local_path, share_name):
        """Remove export"""
        pass

    def access_allow(self, local_path, share_name, access_type, access):
        """Allow access to the host"""
        if access_type != 'ip':
            reason = 'only ip access type allowed'
            raise exception. InvalidShareAccess(reason)
        #check if presents in export
        out, _ = self._execute('exportfs', run_as_root=True)
        out = re.search(re.escape(local_path) + '[\s\n]*' + re.escape(access),
                        out)
        if out is not None:
            raise exception.ShareAccessExists()

        self._execute('exportfs', '-o', 'rw,no_subtree_check',
                      ':'.join([access, local_path]), run_as_root=True,
                      check_exit_code=True)

    def access_deny(self, local_path, share_name, access_type, access,
                    force=False):
        """Deny access to the host"""
        self._execute('exportfs', '-u', ':'.join([access, local_path]),
                      run_as_root=True, check_exit_code=False)


class CIFSHelper(NASHelperBase):
    """Class provides functionality to operate with cifs shares"""

    def __init__(self, execute, config):
        """Store executor and configuration path"""
        super(CIFSHelper, self).__init__(execute)
        self.config = config
        self.test_config = config + '_'

    def init(self):
        """Initialize environment"""
        self._recreate_config()
        self._ensure_daemon_started()

    def create_export(self, local_path, share_name, recreate=False):
        """Create new export, delete old one if exists"""
        parser = ConfigParser.ConfigParser()
        parser.read(self.config)
        #delete old one
        if parser.has_section(share_name):
            if recreate:
                parser.remove_section(share_name)
            else:
                raise exception.Error('Section exists')
        #Create new one
        parser.add_section(share_name)
        parser.set(share_name, 'path', local_path)
        parser.set(share_name, 'browseable', 'yes')
        parser.set(share_name, 'guest ok', 'yes')
        parser.set(share_name, 'read only', 'no')
        parser.set(share_name, 'writable', 'yes')
        parser.set(share_name, 'create mask', '0755')
        parser.set(share_name, 'hosts deny', '0.0.0.0/0')  # denying all ips
        parser.set(share_name, 'hosts allow', '127.0.0.1')
        self._execute('chown', 'nobody', '-R', local_path, run_as_root=True)
        self._update_config(parser)
        return '//%s/%s' % (FLAGS.share_export_ip, share_name)

    def remove_export(self, local_path, share_name):
        """Remove export"""
        parser = ConfigParser.ConfigParser()
        parser.read(self.config)
        #delete old one
        if parser.has_section(share_name):
            parser.remove_section(share_name)
        self._update_config(parser)
        self._execute('smbcontrol', 'all', 'close-share', share_name,
            run_as_root=True)

    def access_allow(self, local_path, share_name, access_type, access):
        """Allow access to the host"""
        if access_type != 'ip':
            reason = 'only ip access type allowed'
            raise exception.InvalidShareAccess(reason)
        parser = ConfigParser.ConfigParser()
        parser.read(self.config)

        hosts = parser.get(share_name, 'hosts allow')
        if access in hosts.split():
            raise exception.ShareAccessExists()
        hosts += ' %s' % (access,)
        parser.set(share_name, 'hosts allow', hosts)
        self._update_config(parser)

    def access_deny(self, local_path, share_name, access_type, access,
                    force=False):
        """Deny access to the host"""
        parser = ConfigParser.ConfigParser()
        try:
            parser.read(self.config)
            hosts = parser.get(share_name, 'hosts allow')
            hosts = hosts.replace(' %s' % (access,), '', 1)
            parser.set(share_name, 'hosts allow', hosts)
            self._update_config(parser)
        except ConfigParser.NoSectionError:
            if  not force:
                raise

    def _ensure_daemon_started(self):
        """
        FYI: smbd starts at least two processes
        """
        out, _ = self._execute(*'ps -C smbd -o args='.split(),
                              check_exit_code=False)
        processes = [process.strip() for process in out.split('\n')
                     if process.strip()]

        cmd = 'smbd -s %s -D' % (self.config,)

        running = False
        for process in processes:
            if not process.endswith(cmd):
                #alternatively exit
                raise exception.Error('smbd already started with wrong config')
            running = True

        if not running:
            self._execute(*cmd.split(), run_as_root=True)

    def _recreate_config(self):
        """create new SAMBA configuration file"""
        if os.path.exists(self.config):
            os.unlink(self.config)
        parser = ConfigParser.ConfigParser()
        parser.add_section('global')
        parser.set('global', 'security', 'user')
        parser.set('global', 'server string', '%h server (Samba, Openstack)')

        self._update_config(parser, restart=False)

    def _update_config(self, parser, restart=True):
        """Check if new configuration is correct and save it"""
        #Check that configuration is correct
        with open(self.test_config, 'w') as fp:
            parser.write(fp)
        self._execute('testparm', '-s', self.test_config,
                      check_exit_code=True)
        #save it
        with open(self.config, 'w') as fp:
            parser.write(fp)
        #restart daemon if necessary
        if restart:
            self._execute(*'pkill -HUP smbd'.split(), run_as_root=True)


class FakeShareDriver(ShareDriver):
    """Logs calls instead of executing."""
    def __init__(self, *args, **kwargs):
        super(FakeShareDriver, self).__init__(execute=self.fake_execute,
            *args, **kwargs)

    def do_setup(self, context):
        """No setup necessary in fake mode."""
        pass

    def initialize_connection(self, volume, connector):
        return {
            'driver_volume_type': 'iscsi',
            'data': {}
        }

    def terminate_connection(self, volume, connector):
        pass

    @staticmethod
    def fake_execute(cmd, *_args, **_kwargs):
        """Execute that simply logs the command."""
        LOG.debug(_("FAKE SHARE: %s"), cmd)
        return (None, None)
