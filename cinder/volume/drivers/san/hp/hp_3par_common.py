# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
#    (c) Copyright 2012-2013 Hewlett-Packard Development Company, L.P.
#    All Rights Reserved.
#
#    Copyright 2012 OpenStack LLC
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
#
"""
Volume driver common utilities for HP 3PAR Storage array

The 3PAR drivers requires 3.1.2 MU2 firmware on the 3PAR array.

You will need to install the python hp3parclient.
sudo pip install hp3parclient

The drivers uses both the REST service and the SSH
command line to correctly operate.  Since the
ssh credentials and the REST credentials can be different
we need to have settings for both.

The drivers requires the use of the san_ip, san_login,
san_password settings for ssh connections into the 3PAR
array.   It also requires the setting of
hp3par_api_url, hp3par_username, hp3par_password
for credentials to talk to the REST service on the 3PAR
array.
"""

import ast
import base64
import json
import paramiko
import pprint
from random import randint
import re
import time
import uuid

from eventlet import greenthread
from hp3parclient import client
from hp3parclient import exceptions as hpexceptions
from oslo.config import cfg

from cinder import context
from cinder import exception
from cinder.openstack.common import excutils
from cinder.openstack.common import log as logging
from cinder import utils
from cinder.volume import volume_types


LOG = logging.getLogger(__name__)

hp3par_opts = [
    cfg.StrOpt('hp3par_api_url',
               default='',
               help="3PAR WSAPI Server Url like "
                    "https://<3par ip>:8080/api/v1"),
    cfg.StrOpt('hp3par_username',
               default='',
               help="3PAR Super user username"),
    cfg.StrOpt('hp3par_password',
               default='',
               help="3PAR Super user password",
               secret=True),
    #TODO(kmartin): Remove hp3par_domain during I release.
    cfg.StrOpt('hp3par_domain',
               default=None,
               help="This option is DEPRECATED and no longer used. "
                    "The 3par domain name to use."),
    cfg.StrOpt('hp3par_cpg',
               default="OpenStack",
               help="The CPG to use for volume creation"),
    cfg.StrOpt('hp3par_cpg_snap',
               default="",
               help="The CPG to use for Snapshots for volumes. "
                    "If empty hp3par_cpg will be used"),
    cfg.StrOpt('hp3par_snapshot_retention',
               default="",
               help="The time in hours to retain a snapshot.  "
                    "You can't delete it before this expires."),
    cfg.StrOpt('hp3par_snapshot_expiration',
               default="",
               help="The time in hours when a snapshot expires "
                    " and is deleted.  This must be larger than expiration"),
    cfg.BoolOpt('hp3par_debug',
                default=False,
                help="Enable HTTP debugging to 3PAR"),
    cfg.ListOpt('hp3par_iscsi_ips',
                default=[],
                help="List of target iSCSI addresses to use.")
]


CONF = cfg.CONF
CONF.register_opts(hp3par_opts)


class HP3PARCommon(object):

    stats = {}

    # Valid values for volume type extra specs
    # The first value in the list is the default value
    valid_prov_values = ['thin', 'full']
    valid_persona_values = ['1 - Generic',
                            '2 - Generic-ALUA',
                            '6 - Generic-legacy',
                            '7 - HPUX-legacy',
                            '8 - AIX-legacy',
                            '9 - EGENERA',
                            '10 - ONTAP-legacy',
                            '11 - VMware',
                            '12 - OpenVMS']
    hp_qos_keys = ['maxIOPS', 'maxBWS']
    hp3par_valid_keys = ['cpg', 'snap_cpg', 'provisioning', 'persona', 'vvs']

    def __init__(self, config):
        self.sshpool = None
        self.config = config
        self.hosts_naming_dict = dict()
        self.client = None
        if CONF.hp3par_domain is not None:
            LOG.deprecated(_("hp3par_domain has been deprecated and "
                             "is no longer used. The domain is automatically "
                             "looked up based on the CPG."))

    def check_flags(self, options, required_flags):
        for flag in required_flags:
            if not getattr(options, flag, None):
                raise exception.InvalidInput(reason=_('%s is not set') % flag)

    def _create_client(self):
        return client.HP3ParClient(self.config.hp3par_api_url)

    def client_login(self):
        try:
            LOG.debug("Connecting to 3PAR")
            self.client.login(self.config.hp3par_username,
                              self.config.hp3par_password)
        except hpexceptions.HTTPUnauthorized as ex:
            LOG.warning("Failed to connect to 3PAR (%s) because %s" %
                       (self.config.hp3par_api_url, str(ex)))
            msg = _("Login to 3PAR array invalid")
            raise exception.InvalidInput(reason=msg)

    def client_logout(self):
        self.client.logout()
        LOG.debug("Disconnect from 3PAR")

    def do_setup(self, context):
        self.client = self._create_client()
        if self.config.hp3par_debug:
            self.client.debug_rest(True)

        self.client_login()

        try:
            # make sure the default CPG exists
            self.validate_cpg(self.config.hp3par_cpg)
            self._set_connections()
        finally:
            self.client_logout()

    def validate_cpg(self, cpg_name):
        try:
            cpg = self.client.getCPG(cpg_name)
        except hpexceptions.HTTPNotFound as ex:
            err = (_("CPG (%s) doesn't exist on array") % cpg_name)
            LOG.error(err)
            raise exception.InvalidInput(reason=err)

    def _set_connections(self):
        """Set the number of concurrent connections.

        The 3PAR WS API server has a limit of concurrent connections.
        This is setting the number to the highest allowed, 15 connections.
        """
        self._cli_run(['setwsapi', '-sru', 'high'])

    def get_domain(self, cpg_name):
        try:
            cpg = self.client.getCPG(cpg_name)
        except hpexceptions.HTTPNotFound:
            err = (_("Failed to get domain because CPG (%s) doesn't "
                     "exist on array.") % cpg_name)
            LOG.error(err)
            raise exception.InvalidInput(reason=err)

        if 'domain' in cpg:
            return cpg['domain']
        return None

    def extend_volume(self, volume, new_size):
        volume_name = self._get_3par_vol_name(volume['id'])
        old_size = volume.size
        growth_size = int(new_size) - old_size
        LOG.debug("Extending Volume %s from %s to %s, by %s GB." %
                  (volume_name, old_size, new_size, growth_size))
        try:
            self._cli_run(['growvv', '-f', volume_name, '%dg' % growth_size])
        except Exception:
            with excutils.save_and_reraise_exception():
                LOG.error(_("Error extending volume %s") % volume)

    def _get_3par_vol_name(self, volume_id):
        """Get converted 3PAR volume name.

        Converts the openstack volume id from
        ecffc30f-98cb-4cf5-85ee-d7309cc17cd2
        to
        osv-7P.DD5jLTPWF7tcwnMF80g

        We convert the 128 bits of the uuid into a 24character long
        base64 encoded string to ensure we don't exceed the maximum
        allowed 31 character name limit on 3Par

        We strip the padding '=' and replace + with .
        and / with -
        """
        volume_name = self._encode_name(volume_id)
        return "osv-%s" % volume_name

    def _get_3par_snap_name(self, snapshot_id):
        snapshot_name = self._encode_name(snapshot_id)
        return "oss-%s" % snapshot_name

    def _get_3par_vvs_name(self, volume_id):
        vvs_name = self._encode_name(volume_id)
        return "vvs-%s" % vvs_name

    def _encode_name(self, name):
        uuid_str = name.replace("-", "")
        vol_uuid = uuid.UUID('urn:uuid:%s' % uuid_str)
        vol_encoded = base64.b64encode(vol_uuid.bytes)

        # 3par doesn't allow +, nor /
        vol_encoded = vol_encoded.replace('+', '.')
        vol_encoded = vol_encoded.replace('/', '-')
        # strip off the == as 3par doesn't like those.
        vol_encoded = vol_encoded.replace('=', '')
        return vol_encoded

    def _capacity_from_size(self, vol_size):

        # because 3PAR volume sizes are in
        # Mebibytes, Gigibytes, not Megabytes.
        MB = 1000L
        MiB = 1.048576

        if int(vol_size) == 0:
            capacity = MB  # default: 1GB
        else:
            capacity = vol_size * MB

        capacity = int(round(capacity / MiB))
        return capacity

    def _cli_run(self, cmd):
        """Runs a CLI command over SSH, without doing any result parsing."""
        LOG.debug("SSH CMD = %s " % cmd)

        (stdout, stderr) = self._run_ssh(cmd, False)

        # we have to strip out the input and exit lines
        tmp = stdout.split("\r\n")
        out = tmp[5:len(tmp) - 2]
        return out

    def _ssh_execute(self, ssh, cmd, check_exit_code=True):
        """We have to do this in order to get CSV output from the CLI command.

        We first have to issue a command to tell the CLI that we want the
        output to be formatted in CSV, then we issue the real command.
        """
        LOG.debug(_('Running cmd (SSH): %s'), cmd)

        channel = ssh.invoke_shell()
        stdin_stream = channel.makefile('wb')
        stdout_stream = channel.makefile('rb')
        stderr_stream = channel.makefile('rb')

        stdin_stream.write('''setclienv csvtable 1
%s
exit
''' % cmd)

        # stdin.write('process_input would go here')
        # stdin.flush()

        # NOTE(justinsb): This seems suspicious...
        # ...other SSH clients have buffering issues with this approach
        stdout = stdout_stream.read()
        stderr = stderr_stream.read()
        stdin_stream.close()
        stdout_stream.close()
        stderr_stream.close()

        exit_status = channel.recv_exit_status()

        # exit_status == -1 if no exit code was returned
        if exit_status != -1:
            LOG.debug(_('Result was %s') % exit_status)
            if check_exit_code and exit_status != 0:
                raise exception.ProcessExecutionError(exit_code=exit_status,
                                                      stdout=stdout,
                                                      stderr=stderr,
                                                      cmd=cmd)
        channel.close()
        return (stdout, stderr)

    def _run_ssh(self, cmd_list, check_exit=True, attempts=1):
        utils.check_ssh_injection(cmd_list)
        command = ' '. join(cmd_list)

        if not self.sshpool:
            self.sshpool = utils.SSHPool(self.config.san_ip,
                                         self.config.san_ssh_port,
                                         self.config.ssh_conn_timeout,
                                         self.config.san_login,
                                         password=self.config.san_password,
                                         privatekey=
                                         self.config.san_private_key,
                                         min_size=
                                         self.config.ssh_min_pool_conn,
                                         max_size=
                                         self.config.ssh_max_pool_conn)
        try:
            total_attempts = attempts
            with self.sshpool.item() as ssh:
                while attempts > 0:
                    attempts -= 1
                    try:
                        return self._ssh_execute(ssh, command,
                                                 check_exit_code=check_exit)
                    except Exception as e:
                        LOG.error(e)
                        greenthread.sleep(randint(20, 500) / 100.0)
                msg = (_("SSH Command failed after '%(total_attempts)r' "
                         "attempts : '%(command)s'") %
                       {'total_attempts': total_attempts, 'command': command})
                raise paramiko.SSHException(msg)
        except Exception:
            with excutils.save_and_reraise_exception():
                LOG.error(_("Error running ssh command: %s") % command)

    def _delete_3par_host(self, hostname):
        self._cli_run(['removehost', hostname])

    def _create_3par_vlun(self, volume, hostname):
        out = self._cli_run(['createvlun', volume, 'auto', hostname])
        if out and len(out) > 1:
            if "must be in the same domain" in out[0]:
                err = out[0].strip()
                err = err + " " + out[1].strip()
                raise exception.Invalid3PARDomain(err=err)

    def _safe_hostname(self, hostname):
        """We have to use a safe hostname length for 3PAR host names."""
        try:
            index = hostname.index('.')
        except ValueError:
            # couldn't find it
            index = len(hostname)

        # we'll just chop this off for now.
        if index > 23:
            index = 23

        return hostname[:index]

    def _get_3par_host(self, hostname):
        out = self._cli_run(['showhost', '-verbose', hostname])
        LOG.debug("OUTPUT = \n%s" % (pprint.pformat(out)))
        host = {'id': None, 'name': None,
                'domain': None,
                'descriptors': {},
                'iSCSIPaths': [],
                'FCPaths': []}

        if out:
            err = out[0]
            if err == 'no hosts listed':
                msg = {'code': 'NON_EXISTENT_HOST',
                       'desc': "HOST '%s' was not found" % hostname}
                raise hpexceptions.HTTPNotFound(msg)

            # start parsing the lines after the header line
            for line in out[1:]:
                if line == '':
                    break
                tmp = line.split(',')
                paths = {}

                LOG.debug("line = %s" % (pprint.pformat(tmp)))
                host['id'] = tmp[0]
                host['name'] = tmp[1]

                portPos = tmp[4]
                LOG.debug("portPos = %s" % (pprint.pformat(portPos)))
                if portPos == '---':
                    portPos = None
                else:
                    port = portPos.split(':')
                    portPos = {'node': int(port[0]), 'slot': int(port[1]),
                               'cardPort': int(port[2])}

                paths['portPos'] = portPos

                # If FC entry
                if tmp[5] == 'n/a':
                    paths['wwn'] = tmp[3]
                    host['FCPaths'].append(paths)
                # else iSCSI entry
                else:
                    paths['name'] = tmp[3]
                    paths['ipAddr'] = tmp[5]
                    host['iSCSIPaths'].append(paths)

            # find the offset to the description stuff
            offset = 0
            for line in out:
                if line[:15] == '---------- Host':
                    break
                else:
                    offset += 1

            info = out[offset + 2]
            tmp = info.split(':')
            host['domain'] = tmp[1]

            info = out[offset + 4]
            tmp = info.split(':')
            host['descriptors']['location'] = tmp[1]

            info = out[offset + 5]
            tmp = info.split(':')
            host['descriptors']['ipAddr'] = tmp[1]

            info = out[offset + 6]
            tmp = info.split(':')
            host['descriptors']['os'] = tmp[1]

            info = out[offset + 7]
            tmp = info.split(':')
            host['descriptors']['model'] = tmp[1]

            info = out[offset + 8]
            tmp = info.split(':')
            host['descriptors']['contact'] = tmp[1]

            info = out[offset + 9]
            tmp = info.split(':')
            host['descriptors']['comment'] = tmp[1]

        return host

    def get_ports(self):
        # First get the active FC ports
        out = self._cli_run(['showport'])

        # strip out header
        # N:S:P,Mode,State,----Node_WWN----,-Port_WWN/HW_Addr-,Type,
        # Protocol,Label,Partner,FailoverState
        out = out[1:len(out) - 2]

        ports = {'FC': [], 'iSCSI': {}}
        for line in out:
            tmp = line.split(',')

            if tmp:
                if tmp[1] == 'target' and tmp[2] == 'ready':
                    if tmp[6] == 'FC':
                        ports['FC'].append(tmp[4])

        # now get the active iSCSI ports
        out = self._cli_run(['showport', '-iscsi'])

        # strip out header
        # N:S:P,State,IPAddr,Netmask,Gateway,
        # TPGT,MTU,Rate,DHCP,iSNS_Addr,iSNS_Port
        out = out[1:len(out) - 2]
        for line in out:
            tmp = line.split(',')

            if tmp and len(tmp) > 2:
                if tmp[1] == 'ready':
                    ports['iSCSI'][tmp[2]] = {}

        # now get the nsp and iqn
        result = self._cli_run(['showport', '-iscsiname'])
        if result:
            # first line is header
            # nsp, ip,iqn
            result = result[1:]
            for line in result:
                info = line.split(",")
                if info and len(info) > 2:
                    if info[1] in ports['iSCSI']:
                        nsp = info[0]
                        ip_addr = info[1]
                        iqn = info[2]
                        ports['iSCSI'][ip_addr] = {'nsp': nsp,
                                                   'iqn': iqn
                                                   }

        LOG.debug("PORTS = %s" % pprint.pformat(ports))
        return ports

    def get_volume_stats(self, refresh):
        if refresh:
            self._update_volume_stats()

        return self.stats

    def _update_volume_stats(self):
        # const to convert MiB to GB
        const = 0.0009765625

        # storage_protocol and volume_backend_name are
        # set in the child classes
        stats = {'driver_version': '1.0',
                 'free_capacity_gb': 'unknown',
                 'reserved_percentage': 0,
                 'storage_protocol': None,
                 'total_capacity_gb': 'unknown',
                 'QoS_support': True,
                 'vendor_name': 'Hewlett-Packard',
                 'volume_backend_name': None}

        try:
            cpg = self.client.getCPG(self.config.hp3par_cpg)
            if 'limitMiB' not in cpg['SDGrowth']:
                total_capacity = 'infinite'
                free_capacity = 'infinite'
            else:
                total_capacity = int(cpg['SDGrowth']['limitMiB'] * const)
                free_capacity = int((cpg['SDGrowth']['limitMiB'] -
                                    cpg['UsrUsage']['usedMiB']) * const)

            stats['total_capacity_gb'] = total_capacity
            stats['free_capacity_gb'] = free_capacity
        except hpexceptions.HTTPNotFound:
            err = (_("CPG (%s) doesn't exist on array")
                   % self.config.hp3par_cpg)
            LOG.error(err)
            raise exception.InvalidInput(reason=err)

        self.stats = stats

    def create_vlun(self, volume, host):
        """Create a VLUN.

        In order to export a volume on a 3PAR box, we have to create a VLUN.
        """
        volume_name = self._get_3par_vol_name(volume['id'])
        self._create_3par_vlun(volume_name, host['name'])
        return self.client.getVLUN(volume_name)

    def delete_vlun(self, volume, hostname):
        volume_name = self._get_3par_vol_name(volume['id'])
        vlun = self.client.getVLUN(volume_name)
        self.client.deleteVLUN(volume_name, vlun['lun'], hostname)
        self._delete_3par_host(hostname)

    def _get_volume_type(self, type_id):
        ctxt = context.get_admin_context()
        return volume_types.get_volume_type(ctxt, type_id)

    def _get_key_value(self, hp3par_keys, key, default=None):
        if hp3par_keys is not None and key in hp3par_keys:
            return hp3par_keys[key]
        else:
            return default

    def _get_qos_value(self, qos, key, default=None):
        if key in qos:
            return qos[key]
        else:
            return default

    def _get_qos_by_volume_type(self, volume_type):
        qos = {}
        specs = volume_type.get('extra_specs')
        for key, value in specs.iteritems():
            if 'qos:' in key:
                fields = key.split(':')
                key = fields[1]
            if key in self.hp_qos_keys:
                qos[key] = int(value)
        return qos

    def _get_keys_by_volume_type(self, volume_type):
        hp3par_keys = {}
        specs = volume_type.get('extra_specs')
        for key, value in specs.iteritems():
            if ':' in key:
                fields = key.split(':')
                key = fields[1]
            if key in self.hp3par_valid_keys:
                hp3par_keys[key] = value
        return hp3par_keys

    def _set_qos_rule(self, qos, vvs_name):
        max_io = self._get_qos_value(qos, 'maxIOPS')
        max_bw = self._get_qos_value(qos, 'maxBWS')
        cmd = ['setqos']
        if max_io is not None:
            cmd.extend(['-io', '%s' % max_io])
        if max_bw is not None:
            cmd.extend(['-bw', '%sM' % max_bw])
        cmd.append('vvset:' + vvs_name)
        self._cli_run(cmd)

    def _add_volume_to_volume_set(self, volume, volume_name,
                                  cpg, vvs_name, qos):
        if vvs_name is not None:
            # Admin has set a volume set name to add the volume to
            self._cli_run(['createvvset', '-add', vvs_name, volume_name])
        else:
            vvs_name = self._get_3par_vvs_name(volume['id'])
            domain = self.get_domain(cpg)
            self._cli_run(['createvvset', '-domain', domain, vvs_name])
            self._set_qos_rule(qos, vvs_name)
            self._cli_run(['createvvset', '-add', vvs_name, volume_name])

    def _remove_volume_set(self, vvs_name):
        # Must first clear the QoS rules before removing the volume set
        self._cli_run(['setqos', '-clear', 'vvset:%s' % (vvs_name)])
        self._cli_run(['removevvset', '-f', vvs_name])

    def _remove_volume_from_volume_set(self, volume_name, vvs_name):
        self._cli_run(['removevvset', '-f', vvs_name, volume_name])

    def get_cpg(self, volume, allowSnap=False):
        volume_name = self._get_3par_vol_name(volume['id'])
        vol = self.client.getVolume(volume_name)
        if 'userCPG' in vol:
            return vol['userCPG']
        elif allowSnap:
            return vol['snapCPG']
        return None

    def _get_3par_vol_comment(self, volume_name):
        vol = self.client.getVolume(volume_name)
        if 'comment' in vol:
            return vol['comment']
        return None

    def get_persona_type(self, volume, hp3par_keys=None):
        default_persona = self.valid_persona_values[0]
        type_id = volume.get('volume_type_id', None)
        volume_type = None
        if type_id is not None:
            volume_type = self._get_volume_type(type_id)
            if hp3par_keys is None:
                hp3par_keys = self._get_keys_by_volume_type(volume_type)
        persona_value = self._get_key_value(hp3par_keys, 'persona',
                                            default_persona)
        if persona_value not in self.valid_persona_values:
            err = _("Must specify a valid persona %(valid)s, "
                    "value '%(persona)s' is invalid.") % \
                   ({'valid': self.valid_persona_values,
                     'persona': persona_value})
            raise exception.InvalidInput(reason=err)
        # persona is set by the id so remove the text and return the id
        # i.e for persona '1 - Generic' returns 1
        persona_id = persona_value.split(' ')
        return persona_id[0]

    def get_volume_settings_from_type(self, volume):
        cpg = None
        snap_cpg = None
        volume_type = None
        vvs_name = None
        hp3par_keys = {}
        qos = {}
        type_id = volume.get('volume_type_id', None)
        if type_id is not None:
            volume_type = self._get_volume_type(type_id)
            hp3par_keys = self._get_keys_by_volume_type(volume_type)
            vvs_name = self._get_key_value(hp3par_keys, 'vvs')
            if vvs_name is None:
                qos = self._get_qos_by_volume_type(volume_type)

        cpg = self._get_key_value(hp3par_keys, 'cpg',
                                  self.config.hp3par_cpg)
        if cpg is not self.config.hp3par_cpg:
            # The cpg was specified in a volume type extra spec so it
            # needs to be validiated that it's in the correct domain.
            self.validate_cpg(cpg)
            # Also, look to see if the snap_cpg was specified in volume
            # type extra spec, if not use the extra spec cpg as the
            # default.
            snap_cpg = self._get_key_value(hp3par_keys, 'snap_cpg', cpg)
        else:
            # default snap_cpg to hp3par_cpg_snap if it's not specified
            # in the volume type extra specs.
            snap_cpg = self.config.hp3par_cpg_snap
            # if it's still not set or empty then set it to the cpg
            # specified in the cinder.conf file.
            if not self.config.hp3par_cpg_snap:
                snap_cpg = cpg

        # if provisioning is not set use thin
        default_prov = self.valid_prov_values[0]
        prov_value = self._get_key_value(hp3par_keys, 'provisioning',
                                         default_prov)
        # check for valid provisioning type
        if prov_value not in self.valid_prov_values:
            err = _("Must specify a valid provisioning type %(valid)s, "
                    "value '%(prov)s' is invalid.") % \
                   ({'valid': self.valid_prov_values,
                     'prov': prov_value})
            raise exception.InvalidInput(reason=err)

        tpvv = True
        if prov_value == "full":
            tpvv = False

        # check for valid persona even if we don't use it until
        # attach time, this will give the end user notice that the
        # persona type is invalid at volume creation time
        self.get_persona_type(volume, hp3par_keys)

        return {'cpg': cpg, 'snap_cpg': snap_cpg,
                'vvs_name': vvs_name, 'qos': qos,
                'tpvv': tpvv, 'volume_type': volume_type}

    def create_volume(self, volume):
        LOG.debug("CREATE VOLUME (%s : %s %s)" %
                  (volume['display_name'], volume['name'],
                   self._get_3par_vol_name(volume['id'])))
        try:
            comments = {'volume_id': volume['id'],
                        'name': volume['name'],
                        'type': 'OpenStack'}

            name = volume.get('display_name', None)
            if name:
                comments['display_name'] = name

            # get the options supported by volume types
            type_info = self.get_volume_settings_from_type(volume)
            volume_type = type_info['volume_type']
            vvs_name = type_info['vvs_name']
            qos = type_info['qos']
            cpg = type_info['cpg']
            snap_cpg = type_info['snap_cpg']
            tpvv = type_info['tpvv']

            type_id = volume.get('volume_type_id', None)
            if type_id is not None:
                comments['volume_type_name'] = volume_type.get('name')
                comments['volume_type_id'] = type_id
                if vvs_name is not None:
                    comments['vvs'] = vvs_name
                else:
                    comments['qos'] = qos

            extras = {'comment': json.dumps(comments),
                      'snapCPG': snap_cpg,
                      'tpvv': tpvv}

            capacity = self._capacity_from_size(volume['size'])
            volume_name = self._get_3par_vol_name(volume['id'])
            self.client.createVolume(volume_name, cpg, capacity, extras)
            if qos or vvs_name is not None:
                try:
                    self._add_volume_to_volume_set(volume, volume_name,
                                                   cpg, vvs_name, qos)
                except Exception as ex:
                    # Delete the volume if unable to add it to the volume set
                    self.client.deleteVolume(volume_name)
                    LOG.error(str(ex))
                    raise exception.CinderException(ex.get_description())
        except hpexceptions.HTTPConflict:
            raise exception.Duplicate(_("Volume (%s) already exists on array")
                                      % volume_name)
        except hpexceptions.HTTPBadRequest as ex:
            LOG.error(str(ex))
            raise exception.Invalid(ex.get_description())
        except exception.InvalidInput as ex:
            LOG.error(str(ex))
            raise ex
        except Exception as ex:
            LOG.error(str(ex))
            raise exception.CinderException(ex.get_description())

    def _copy_volume(self, src_name, dest_name, cpg=None, snap_cpg=None,
                     tpvv=True):
        # Virtual volume sets are not supported with the -online option
        cmd = ['createvvcopy', '-p', src_name, '-online']
        if snap_cpg:
            cmd.extend(['-snp_cpg', snap_cpg])
        if tpvv:
            cmd.append('-tpvv')
        if cpg:
            cmd.append(cpg)
        cmd.append(dest_name)
        LOG.debug('Creating clone of a volume with %s' % cmd)
        self._cli_run(cmd)

    def get_next_word(self, s, search_string):
        """Return the next word.

        Search 's' for 'search_string', if found return the word preceding
        'search_string' from 's'.
        """
        word = re.search(search_string.strip(' ') + ' ([^ ]*)', s)
        return word.groups()[0].strip(' ')

    def _get_3par_vol_comment_value(self, vol_comment, key):
        comment_dict = dict(ast.literal_eval(vol_comment))
        if key in comment_dict:
            return comment_dict[key]
        return None

    def create_cloned_volume(self, volume, src_vref):
        try:
            orig_name = self._get_3par_vol_name(volume['source_volid'])
            vol_name = self._get_3par_vol_name(volume['id'])

            type_info = self.get_volume_settings_from_type(volume)

            # make the 3PAR copy the contents.
            # can't delete the original until the copy is done.
            self._copy_volume(orig_name, vol_name, cpg=type_info['cpg'],
                              snap_cpg=type_info['snap_cpg'],
                              tpvv=type_info['tpvv'])
            return None
        except hpexceptions.HTTPForbidden:
            raise exception.NotAuthorized()
        except hpexceptions.HTTPNotFound:
            raise exception.NotFound()
        except Exception as ex:
            LOG.error(str(ex))
            raise exception.CinderException(ex)

    def _get_vvset_from_3par(self, volume_name):
        """Get Virtual Volume Set from 3PAR.

        The only way to do this currently is to try and delete the volume
        to get the error message.

        NOTE(walter-boring): don't call this unless you know the volume is
        already in a vvset!
        """
        cmd = ['removevv', '-f', volume_name]
        LOG.debug("Issuing remove command to find vvset name %s" % cmd)
        out = self._cli_run(cmd)
        vvset_name = None
        if out and len(out) > 1:
            if out[1].startswith("Attempt to delete "):
                words = out[1].split(" ")
                vvset_name = words[len(words) - 1]

        return vvset_name

    def delete_volume(self, volume):
        try:
            volume_name = self._get_3par_vol_name(volume['id'])
            # Try and delete the volume, it might fail here because
            # the volume is part of a volume set which will have the
            # volume set name in the error.
            try:
                self.client.deleteVolume(volume_name)
            except hpexceptions.HTTPConflict as ex:
                if ex.get_code() == 34:
                    # This is a special case which means the
                    # volume is part of a volume set.
                    vvset_name = self._get_vvset_from_3par(volume_name)
                    LOG.debug("Returned vvset_name = %s" % vvset_name)
                    if vvset_name is not None and \
                       vvset_name.startswith('vvs-'):
                        # We have a single volume per volume set, so
                        # remove the volume set.
                        self._remove_volume_set(
                            self._get_3par_vvs_name(volume['id']))
                    elif vvset_name is not None:
                        # We have a pre-defined volume set just remove the
                        # volume and leave the volume set.
                        self._remove_volume_from_volume_set(volume_name,
                                                            vvset_name)
                    self.client.deleteVolume(volume_name)
                else:
                    raise ex

        except hpexceptions.HTTPNotFound as ex:
            # We'll let this act as if it worked
            # it helps clean up the cinder entries.
            LOG.error(str(ex))
        except hpexceptions.HTTPForbidden as ex:
            LOG.error(str(ex))
            raise exception.NotAuthorized(ex.get_description())
        except Exception as ex:
            LOG.error(str(ex))
            raise exception.CinderException(ex)

    def create_volume_from_snapshot(self, volume, snapshot):
        """Creates a volume from a snapshot.

        TODO: support using the size from the user.
        """
        LOG.debug("Create Volume from Snapshot\n%s\n%s" %
                  (pprint.pformat(volume['display_name']),
                   pprint.pformat(snapshot['display_name'])))

        if snapshot['volume_size'] != volume['size']:
            err = "You cannot change size of the volume.  It must "
            "be the same as the snapshot."
            LOG.error(err)
            raise exception.InvalidInput(reason=err)

        try:
            snap_name = self._get_3par_snap_name(snapshot['id'])
            volume_name = self._get_3par_vol_name(volume['id'])

            extra = {'volume_id': volume['id'],
                     'snapshot_id': snapshot['id']}

            volume_type = None
            type_id = volume.get('volume_type_id', None)
            vvs_name = None
            qos = {}
            hp3par_keys = {}
            if type_id is not None:
                volume_type = self._get_volume_type(type_id)
                hp3par_keys = self._get_keys_by_volume_type(volume_type)
                vvs_name = self._get_key_value(hp3par_keys, 'vvs')
                if vvs_name is None:
                    qos = self._get_qos_by_volume_type(volume_type)

            name = volume.get('display_name', None)
            if name:
                extra['display_name'] = name

            description = volume.get('display_description', None)
            if description:
                extra['description'] = description

            optional = {'comment': json.dumps(extra),
                        'readOnly': False}

            self.client.createSnapshot(volume_name, snap_name, optional)
            if qos or vvs_name is not None:
                cpg = self._get_key_value(hp3par_keys, 'cpg',
                                          self.config.hp3par_cpg)
                try:
                    self._add_volume_to_volume_set(volume, volume_name,
                                                   cpg, vvs_name, qos)
                except Exception as ex:
                    # Delete the volume if unable to add it to the volume set
                    self.client.deleteVolume(volume_name)
                    LOG.error(str(ex))
                    raise exception.CinderException(ex.get_description())
        except hpexceptions.HTTPForbidden:
            raise exception.NotAuthorized()
        except hpexceptions.HTTPNotFound:
            raise exception.NotFound()
        except Exception as ex:
            LOG.error(str(ex))
            raise exception.CinderException(ex.get_description())

    def create_snapshot(self, snapshot):
        LOG.debug("Create Snapshot\n%s" % pprint.pformat(snapshot))

        try:
            snap_name = self._get_3par_snap_name(snapshot['id'])
            vol_name = self._get_3par_vol_name(snapshot['volume_id'])

            extra = {'volume_name': snapshot['volume_name']}
            vol_id = snapshot.get('volume_id', None)
            if vol_id:
                extra['volume_id'] = vol_id

            try:
                extra['display_name'] = snapshot['display_name']
            except AttributeError:
                pass

            try:
                extra['description'] = snapshot['display_description']
            except AttributeError:
                pass

            optional = {'comment': json.dumps(extra),
                        'readOnly': True}
            if self.config.hp3par_snapshot_expiration:
                optional['expirationHours'] = (
                    self.config.hp3par_snapshot_expiration)

            if self.config.hp3par_snapshot_retention:
                optional['retentionHours'] = (
                    self.config.hp3par_snapshot_retention)

            self.client.createSnapshot(snap_name, vol_name, optional)
        except hpexceptions.HTTPForbidden:
            raise exception.NotAuthorized()
        except hpexceptions.HTTPNotFound:
            raise exception.NotFound()

    def delete_snapshot(self, snapshot):
        LOG.debug("Delete Snapshot\n%s" % pprint.pformat(snapshot))

        try:
            snap_name = self._get_3par_snap_name(snapshot['id'])
            self.client.deleteVolume(snap_name)
        except hpexceptions.HTTPForbidden:
            raise exception.NotAuthorized()
        except hpexceptions.HTTPNotFound as ex:
            LOG.error(str(ex))

    def _get_3par_hostname_from_wwn_iqn(self, wwns_iqn):
        out = self._cli_run(['showhost', '-d'])
        # wwns_iqn may be a list of strings or a single
        # string. So, if necessary, create a list to loop.
        if not isinstance(wwns_iqn, list):
            wwn_iqn_list = [wwns_iqn]
        else:
            wwn_iqn_list = wwns_iqn

        for wwn_iqn in wwn_iqn_list:
            for showhost in out:
                if (wwn_iqn.upper() in showhost.upper()):
                    return showhost.split(',')[1]

    def terminate_connection(self, volume, hostname, wwn_iqn):
        """Driver entry point to unattach a volume from an instance."""
        try:
            # does 3par know this host by a different name?
            if hostname in self.hosts_naming_dict:
                hostname = self.hosts_naming_dict.get(hostname)
            self.delete_vlun(volume, hostname)
            return
        except hpexceptions.HTTPNotFound as e:
            if 'host does not exist' in e.get_description():
                # use the wwn to see if we can find the hostname
                hostname = self._get_3par_hostname_from_wwn_iqn(wwn_iqn)
                # no 3par host, re-throw
                if (hostname is None):
                    raise
            else:
            # not a 'host does not exist' HTTPNotFound exception, re-throw
                raise

        #try again with name retrieved from 3par
        self.delete_vlun(volume, hostname)

    def parse_create_host_error(self, hostname, out):
        search_str = "already used by host "
        if search_str in out[1]:
            #host exists, return name used by 3par
            hostname_3par = self.get_next_word(out[1], search_str)
            self.hosts_naming_dict[hostname] = hostname_3par
            return hostname_3par
