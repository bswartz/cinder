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
NetApp specific NAS storage driver. Supports NFS and CIFS protocols.

This driver requires NetApp OnCommand 5.0 and one or more Data
ONTAP 7-mode storage systems with installed CIFS and NFS licenses.
"""
from cinder import exception
from cinder import flags
from cinder.openstack.common import cfg
from cinder.openstack.common import log
from suds.sax import text
import suds

LOG = log.getLogger(__name__)

NETAPP_NAS_OPTS = [
    cfg.StrOpt('netapp_nas_wsdl_url',
                default=None,
                help='URL of the WSDL file for the DFM server'),
    cfg.StrOpt('netapp_nas_login',
               default=None,
               help='User name for the DFM server'),
    cfg.StrOpt('netapp_nas_password',
               default=None,
               help='Password for the DFM server'),
    cfg.StrOpt('netapp_nas_server_hostname',
               default=None,
               help='Hostname for the DFM server'),
    cfg.IntOpt('netapp_nas_server_port',
               default=8088,
               help='Port number for the DFM server')
]

FLAGS = flags.FLAGS
FLAGS.register_opts(NETAPP_NAS_OPTS)


class NetAppNASDriver(object):
    """
    NetApp specific NAS driver. Allows for NFS and CIFS NAS storage usage.
    """

    def __init__(self, *args, **kwargs):
        super(NetAppNASDriver, self).__init__(*args, **kwargs)
        self._client = NetAppApiClient()
        self._helpers = None
        self.db = None
        self._share_table = {}

    def check_for_setup_error(self):
        """Raises error if prerequisites are not met"""
        self._client.check_configuration()

    def ensure_export(self, context, volume):
        """Synchronously recreates an export for a volume."""
        pass

    def get_volume_stats(self, refresh=False):
        """Returns volume stats"""
        pass

    def create_volume(self, volume):
        """Creates volume"""
        aggregate = self._find_best_aggregate()
        filer = aggregate.FilerId
        self._create_volume_on(aggregate, volume)
        self._remember_share(volume['id'], filer)

    def delete_volume(self, volume):
        """Deletes volume"""
        volume_id = volume['id']
        target = self._get_filer(volume_id)
        if target:
            self._volume_offline(target, volume)
            self._delete_volume(target, volume)
        self._forget_share(volume_id)

    def create_export(self, context, volume):
        """Creates export"""
        pass

    def remove_export(self, context, volume):
        """Removes export"""
        pass

    def do_setup(self, context):
        """
        Called once by the manager after the driver is loaded.
        Validate the flags we care about and setup the suds (web services)
        client.
        """
        self._client.do_setup()
        self._setup_helpers()
        self._find_existing_shares(context)

    def create_share(self, context, share, volume):
        """Creates NAS storage"""
        filer = self._get_filer(volume['id'])
        export_location = self._do_create_share(filer, share, volume)

        return export_location

    def delete_share(self, context, share, volume):
        """Deletes NAS storage"""
        helper = self._get_helper(share)
        target = helper.get_target(share)
        rules = self.db.share_access_get_all_for_share(context, volume['id'])

        for rule in rules:
            try:
                self.access_deny(context, share, volume, rule)
            except exception.Error:
                LOG.info(_("Cannot remove access rule %d") % rule.id)

        # share may be in error state, so there's no volume and target
        if target:
            helper.delete_share(share, volume)

    def access_allow(self, context, share, volume, access):
        """Allows access to a given NAS storage for IPs in :access:"""
        helper = self._get_helper(share)
        return helper.access_allow(context, share, volume, access)

    def access_deny(self, context, share, volume, access):
        """Denies access to a given NAS storage for IPs in :access:"""
        helper = self._get_helper(share)
        return helper.access_deny(context, share, volume, access)

    def create_snapshot(self, snapshot):
        """Creates a snapshot of a NAS share"""
        volume_id = snapshot['volume_id']
        volume_name = _get_valid_netapp_volume_name(volume_id)
        filer = self._get_filer(volume_id)

        xml_args = ('<volume>%s</volume>'
                    '<snapshot>%s</snapshot>') % (volume_name,
                                                  snapshot['name'])
        self._client.send_request_to(filer, 'snapshot-create', xml_args)

    def delete_snapshot(self, snapshot):
        """Deletes snapshot"""
        volume_id = snapshot['volume_id']
        volume_name = _get_valid_netapp_volume_name(volume_id)
        filer = self._get_filer(volume_id)
        snapshot_name = snapshot['name']

        self._is_snapshot_busy(filer, volume_name, snapshot_name)

        xml_args = ('<snapshot>%s</snapshot>'
                    '<volume>%s</volume>') % (snapshot_name, volume_name)
        self._client.send_request_to(filer, 'snapshot-delete', xml_args)

    def create_volume_from_snapshot(self, volume, snapshot):
        """Creates NAS share from a snapshot"""
        parent_volume_id = snapshot['volume_id']
        parent_snapshot = snapshot['name']
        parent_volume_name = _get_valid_netapp_volume_name(parent_volume_id)
        volume_id = volume['id']
        volume_name = _get_valid_netapp_volume_name(volume_id)

        filer = self._get_filer(parent_volume_id)

        xml_args = ('<volume>%s</volume>'
                    '<parent-volume>%s</parent-volume>'
                    '<parent-snapshot>%s</parent-snapshot>') % \
                   (volume_name, parent_volume_name, parent_snapshot)

        self._client.send_request_to(filer,
                                     'volume-clone-create',
                                     xml_args)
        self._remember_share(volume_id, filer)

    def ensure_share(self, context, share, volume):
        """Ensure share is available"""
        pass

    def _do_create_share(self, target_id, share, volume):
        helper = self._get_helper(share)
        export_location = helper.create_share(target_id, share, volume)

        return export_location

    def _is_snapshot_busy(self, filer, volume_name, snapshot_name):
        """Raises SnapshotIsBusy exception if snapshot is not available"""
        xml_args = ('<volume>%s</volume>') % volume_name
        snapshots = self._client.send_request_to(filer,
                                                 'snapshot-list-info',
                                                 xml_args,
                                                 do_response_check=False)

        for snap in snapshots.Results.snapshots[0]['snapshot-info']:
            if snap['name'][0] == snapshot_name and snap['busy'][0] == 'true':
                raise exception.SnapshotIsBusy('Snapshot %s is busy.' %
                                               snapshot_name)

    def _get_filer(self, volume_id):
        """Returns filer name for the volume_id"""
        filer = None

        try:
            filer = self._share_table[volume_id]
        except KeyError:
            pass

        return filer

    def _remember_share(self, volume_id, filer):
        """Stores required share info in local dictionary"""
        self._share_table[volume_id] = filer

    def _forget_share(self, volume_id):
        """Remove share info about share"""
        try:
            self._share_table.pop(volume_id)
        except KeyError:
            pass

    def _find_existing_shares(self, context):
        """Retrieves all previously created shares from DB"""
        volumes = self.db.volume_get_all(context)

        for volume in volumes:
            if volume['status'] != 'available':
                continue

            try:
                volume_id = volume['id']
                share = self.db.share_get_by_volume_id(context, volume_id)
                helper = self._get_helper(share)
                filer = helper.get_target(share)
                self._remember_share(volume_id, filer)
            except exception.NotFound:
                pass

    def _volume_offline(self, target, volume):
        """Sends volume offline. Required before deleting a volume."""
        volume_name = _get_valid_netapp_volume_name(volume['id'])

        xml_args = ('<name>%s</name>') % volume_name

        self._client.send_request_to(target, 'volume-offline', xml_args)

    def _delete_volume(self, target, volume):
        """Destroys volume on a target OnTap device"""
        volume_name = _get_valid_netapp_volume_name(volume['id'])
        xml_args = ('<force>true</force>'
                    '<name>%s</name>') % volume_name

        self._client.send_request_to(target, 'volume-destroy', xml_args)

    def _setup_helpers(self):
        """Initializes protocol-specific NAS drivers"""
        suds_client = self._client
        self._helpers = {'CIFS': NetAppCIFSHelper(suds_client),
                         'NFS': NetAppNFSHelper(suds_client)}

    def _get_helper(self, share):
        """Returns driver which implements share protocol."""
        share_proto = share['proto']

        for proto in self._helpers.keys():
            if share_proto.upper().startswith(proto):
                return self._helpers[proto]

        err_msg = _("Invalid NAS protocol supplied: %s. ") % (share_proto)

        raise exception.Error(err_msg)

    def _find_best_aggregate(self):
        """
        :returns aggregate with the most free space left.
        """
        aggrs = self._client.get_available_aggregates()

        if aggrs is None:
            raise exception.Error(_("No aggregates available"))

        # Find aggregate with the most free space
        best_aggregate = max(aggrs.Aggregates.AggregateInfo,
                             key=lambda ai: ai.AggregateSize.SizeAvailable)

        return best_aggregate

    def _create_volume_on(self, aggregate, volume):
        """Creates new volume on aggregate. Created volume will be used as
        NAS share."""
        filer_id = aggregate.FilerId

        aggr_name = aggregate.AggregateName.split(':')[1]
        size = volume['size']
        volume_name = _get_valid_netapp_volume_name(volume['id'])

        args_xml = ('<containing-aggr-name>%s</containing-aggr-name>'
                    '<size>%dg</size>'
                    '<volume>%s</volume>') % (aggr_name, size, volume_name)

        self._client.send_request_to(filer_id, 'volume-create', args_xml)


def _check_response(request, response):
    """Checks RPC responses from NetApp devices"""
    if response.Status == 'failed':
        name = request.Name
        reason = response.Reason
        msg = _('API %(name)s failed: %(reason)s')
        raise exception.Error(msg % locals())


def _get_valid_netapp_volume_name(volume_id):
    """The volume name can contain letters, numbers, and the underscore
    character (_). The first character must be a letter or an
    underscore."""
    return 'share_' + volume_id.replace('-', '_')


class NetAppApiClient(object):
    """Wrapper around DFM commands"""

    REQUIRED_FLAGS = ['netapp_nas_wsdl_url',
                      'netapp_nas_login',
                      'netapp_nas_password',
                      'netapp_nas_server_hostname',
                      'netapp_nas_server_port']

    def __init__(self):
        self._client = None

    def do_setup(self):
        """Setup suds (web services) client"""
        soap_url = 'http://%s:%s/apis/soap/v1' % \
                            (FLAGS.netapp_nas_server_hostname,
                             FLAGS.netapp_nas_server_port)

        self._client = suds.client.Client(FLAGS.netapp_nas_wsdl_url,
                                          username=FLAGS.netapp_nas_login,
                                          password=FLAGS.netapp_nas_password,
                                          location=soap_url)

        LOG.info('NetApp RPC client started')

    def send_request_to(self, target, request, xml_args=None,
                          do_response_check=True):
        """
        Sends RPC :request: to :target:.
        :param target: IP address, ID or network name of OnTap device
        :param request: API name
        :param xml_args: call arguments
        :param do_response_check: if set to True and RPC call has failed,
        raises exception.
        """
        client = self._client
        srv = client.service

        rpc = client.factory.create('Request')
        rpc.Name = request
        rpc.Args = text.Raw(xml_args)
        response = srv.ApiProxy(Request=rpc, Target=target)

        if do_response_check:
            _check_response(rpc, response)

        return response

    def get_available_aggregates(self):
        """Returns list of aggregates known by DFM"""
        srv = self._client.service
        resp = srv.AggregateListInfoIterStart()
        tag = resp.Tag

        try:
            avail_aggrs = srv.AggregateListInfoIterNext(Tag=tag,
                                                        Maximum=resp.Records)
        finally:
            srv.AggregateListInfoIterEnd(tag)

        return avail_aggrs

    def get_host_ip_by(self, host_id):
        """Returns IP address of a host known by DFM"""
        if (type(host_id) is str or type(host_id) is unicode) and \
           len(host_id.split('.')) == 4:
            # already IP
            return host_id

        client = self._client
        srv = client.service

        filer_filter = client.factory.create('HostListInfoIterStart')
        filer_filter.ObjectNameOrId = host_id
        resp = srv.HostListInfoIterStart(HostListInfoIterStart=filer_filter)
        tag = resp.Tag

        try:
            filers = srv.HostListInfoIterNext(Tag=tag, Maximum=resp.Records)
        finally:
            srv.HostListInfoIterEnd(Tag=tag)

        ip = None
        for host in filers.Hosts.HostInfo:
            if int(host.HostId) == int(host_id):
                ip = host.HostAddress

        return ip

    @staticmethod
    def check_configuration():
        """Ensure that the flags we care about are set."""
        for flag in NetAppApiClient.REQUIRED_FLAGS:
            if not getattr(FLAGS, flag, None):
                raise exception.Error(_('%s is not set') % flag)


class NetAppNASHelperBase(object):
    """Interface for protocol-specific NAS drivers"""
    def __init__(self, suds_client):
        self._client = suds_client

    def create_share(self, target_id, share, volume):
        """Creates NAS share"""
        raise NotImplementedError()

    def delete_share(self, share, volume):
        """Deletes NAS share"""
        raise NotImplementedError()

    def access_allow(self, context, share, volume, new_rules):
        """Allows new_rules to a given NAS storage for IPs in :new_rules"""
        raise NotImplementedError()

    def access_deny(self, context, share, volume, new_rules):
        """Denies new_rules to a given NAS storage for IPs in :new_rules"""
        raise NotImplementedError()

    def get_target(self, share):
        """Returns host where the share located"""
        raise NotImplementedError()


class NetAppNFSHelper(NetAppNASHelperBase):
    """Netapp specific NFS sharing driver"""

    def __init__(self, suds_client):
        super(NetAppNFSHelper, self).__init__(suds_client)

    def create_share(self, target_id, share, volume):
        """Creates NFS share"""
        args_xml = ('<rules>'
                    '<exports-rule-info-2>'
                        '<pathname>%s</pathname>'
                        '<security-rules>'
                          '<security-rule-info>'
                            '<read-write>'
                               '<exports-hostname-info>'
                                  '<name>localhost</name>'
                               '</exports-hostname-info>'
                            '</read-write>'
                            '<root>'
                              '<exports-hostname-info>'
                                '<all-hosts>false</all-hosts>'
                                '<name>localhost</name>'
                              '</exports-hostname-info>'
                            '</root>'
                          '</security-rule-info>'
                        '</security-rules>'
                    '</exports-rule-info-2>'
                    '</rules>')

        client = self._client
        valid_volume_name = _get_valid_netapp_volume_name(volume['id'])
        export_pathname = '/vol/' + valid_volume_name

        client.send_request_to(target_id, 'nfs-exportfs-append-rules-2',
                               args_xml % export_pathname)

        export_ip = client.get_host_ip_by(target_id)
        export_location = ':'.join([export_ip, export_pathname])
        return export_location

    def delete_share(self, share, volume):
        """Deletes NFS share"""
        target, export_path = self._get_export_path(share)

        xml_args = ('<pathnames>'
                      '<pathname-info>'
                        '<name>%s</name>'
                      '</pathname-info>'
                    '</pathnames>') % export_path

        self._client.send_request_to(target, 'nfs-exportfs-delete-rules',
                                     xml_args)

    def access_allow(self, context, share, volume, access):
        """Allows access to a given NFS storage for IPs in :access:"""
        if access['access_type'] != 'ip':
            raise exception.Error(('Invalid access type supplied. '
                                   'Only \'ip\' type is supported'))

        ips = access['access_to']

        existing_rules = self._get_exisiting_rules(share)
        new_rules_xml = self._append_new_rules_to(existing_rules, ips)

        self._modify_rule(share, new_rules_xml)

    def access_deny(self, context, share, volume, access):
        """Denies access to a given NFS storage for IPs in :access:"""
        denied_ips = access['access_to']
        existing_rules = self._get_exisiting_rules(share)

        if type(denied_ips) is not list:
            denied_ips = [denied_ips]

        for deny_rule in denied_ips:
            try:
                existing_rules.remove(deny_rule)
            except ValueError:
                pass

        new_rules_xml = self._append_new_rules_to([], existing_rules)
        self._modify_rule(share, new_rules_xml)

    def get_target(self, share):
        """Returns ID of target OnTap device based on export location"""
        return self._get_export_path(share)[0]

    def _modify_rule(self, share, rw_rules):
        """Modifies access rule for a share"""
        target, export_path = self._get_export_path(share)

        xml_args = ('<persistent>true</persistent>'
                    '<rules>'
                      '<exports-rule-info-2>'
                        '<pathname>%s</pathname>'
                        '<security-rules>%s'
                        '</security-rules>'
                      '</exports-rule-info-2>'
                    '</rules>') % (export_path, ''.join(rw_rules))

        self._client.send_request_to(target, 'nfs-exportfs-append-rules-2',
                                     xml_args)

    def _get_exisiting_rules(self, share):
        """Returns available access rules for the share"""
        target, export_path = self._get_export_path(share)
        xml_args = '<pathname>%s</pathname>' % export_path

        response = self._client.send_request_to(target,
                                                'nfs-exportfs-list-rules-2',
                                                xml_args)

        rules = response.Results.rules[0]
        security_rule = rules['exports-rule-info-2'][0]['security-rules'][0]
        security_info = security_rule['security-rule-info'][0]
        root_rules = security_info['root'][0]
        allowed_hosts = root_rules['exports-hostname-info']

        existing_rules = []

        for allowed_host in allowed_hosts:
            if 'name' in allowed_host:
                existing_rules.append(allowed_host['name'][0])

        return existing_rules

    @staticmethod
    def _append_new_rules_to(existing_rules, new_rules):
        """Adds new rules to existing"""
        security_rule_xml = ('<security-rule-info>'
                               '<read-write>%s'
                               '</read-write>'
                               '<root>%s'
                               '</root>'
                             '</security-rule-info>')

        hostname_info_xml = ('<exports-hostname-info>'
                                '<name>%s</name>'
                             '</exports-hostname-info>')

        allowed_hosts_xml = []

        if type(new_rules) is not list:
            new_rules = [new_rules]

        all_rules = existing_rules + new_rules

        for ip in all_rules:
            allowed_hosts_xml.append(hostname_info_xml % ip)

        return security_rule_xml % (allowed_hosts_xml, allowed_hosts_xml)

    @staticmethod
    def _get_export_path(share):
        """Returns IP address and export location of a share"""
        export_location = share['export_location']

        if export_location is None:
            export_location = ':'

        return export_location.split(':')


class NetAppCIFSHelper(NetAppNASHelperBase):
    """Netapp specific NFS sharing driver"""

    CIFS_USER_GROUP = 'Administrators'

    def __init__(self, suds_client):
        super(NetAppCIFSHelper, self).__init__(suds_client)

    def create_share(self, target_id, share, volume):
        """Creates CIFS storage"""
        cifs_status = self._get_cifs_status(target_id)

        if cifs_status == 'stopped':
            self._start_cifs_service(target_id)

        volume_name = _get_valid_netapp_volume_name(volume['id'])

        self._set_qtree_security(target_id, volume)
        self._add_share(target_id, volume_name)
        self._restrict_access(target_id, 'everyone', volume_name)

        ip_address = self._client.get_host_ip_by(target_id)

        cifs_location = self._set_export_location(ip_address, volume_name)

        return cifs_location

    def delete_share(self, share, volume):
        """Deletes CIFS storage"""
        host_ip, share_name = self._get_export_location(share)
        xml_args = '<share-name>%s</share-name>' % share_name
        self._client.send_request_to(host_ip, 'cifs-share-delete', xml_args)

    def access_allow(self, context, share, volume, access):
        """Allows access to a given CIFS storage for IPs in :access"""
        if access['access_type'] != 'passwd':
            ex_text = ('NetApp only supports "passwd" access type for CIFS.')
            raise exception.Error(ex_text)

        user = access['access_to']
        target, share_name = self._get_export_location(share)

        if self._user_exists(target, user):
            self._allow_access_for(target, user, share_name)
        else:
            exc_text = ('User "%s" does not exist on %s OnTap.') % (user,
                                                                    target)
            raise exception.Error(exc_text)

    def access_deny(self, context, share, volume, access):
        """Denies access to a given CIFS storage for IPs in access"""
        host_ip, share_name = self._get_export_location(share)
        user = access['access_to']

        self._restrict_access(host_ip, user, share_name)

    def get_target(self, share):
        """Returns OnTap target IP based on share export location"""
        return self._get_export_location(share)[0]

    def _set_qtree_security(self, target, volume):
        client = self._client
        volume_name = '/vol/' + _get_valid_netapp_volume_name(volume['id'])

        xml_args = ('<args>'
                        '<arg>qtree</arg>'
                        '<arg>security</arg>'
                        '<arg>%s</arg>'
                        '<arg>mixed</arg>'
                    '</args>') % volume_name

        client.send_request_to(target, 'system-cli', xml_args)

    def _restrict_access(self, target, user_name, share_name):
        xml_args = ('<user-name>%s</user-name>'
                    '<share-name>%s</share-name>') % (user_name, share_name)
        self._client.send_request_to(target, 'cifs-share-ace-delete',
                                     xml_args)

    def _start_cifs_service(self, target_id):
        """Starts CIFS service on OnTap target"""
        client = self._client
        return client.send_request_to(target_id, 'cifs-start',
                                      do_response_check=False)

    @staticmethod
    def _get_export_location(share):
        """Returns export location for a given CIFS share"""
        export_location = share['export_location']

        if export_location is None:
            export_location = '///'

        _, _, host_ip, share_name = export_location.split('/')
        return host_ip, share_name

    @staticmethod
    def _set_export_location(ip, share_name):
        """Returns export location of a share"""
        return "//%s/%s" % (ip, share_name)

    def _get_cifs_status(self, target_id):
        """Returns status of a CIFS service on target OnTap"""
        client = self._client
        response = client.send_request_to(target_id, 'cifs-status')
        return response.Status

    def _allow_access_for(self, target, username, share_name):
        """Allows access to the CIFS share for a given user"""
        xml_args = ('<access-rights>rwx</access-rights>'
                    '<share-name>%s</share-name>'
                    '<user-name>%s</user-name>') % (share_name, username)
        self._client.send_request_to(target, 'cifs-share-ace-set', xml_args)

    def _user_exists(self, target, user):
        """Returns True if user already exists on a target OnTap"""
        xml_args = ('<user-name>%s</user-name>') % user
        resp = self._client.send_request_to(target,
                                            'useradmin-user-list',
                                            xml_args,
                                            do_response_check=False)

        return (resp.Status == 'passed')

    def _add_share(self, target_id, volume_name):
        """Creates CIFS share on target OnTap host"""
        client = self._client
        xml_args = ('<path>/vol/%s</path>'
                    '<share-name>%s</share-name>') % (volume_name, volume_name)
        client.send_request_to(target_id, 'cifs-share-add', xml_args)
