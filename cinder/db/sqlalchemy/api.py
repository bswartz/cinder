# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright (c) 2011 X.commerce, a business unit of eBay Inc.
# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
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

"""Implementation of SQLAlchemy backend."""

import datetime
import warnings

from cinder import db
from cinder import exception
from cinder import flags
from cinder import utils
from cinder.openstack.common import log as logging
from cinder.db.sqlalchemy import models
from cinder.db.sqlalchemy.session import get_session
from cinder.openstack.common import timeutils
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import joinedload
from sqlalchemy.sql import func
from sqlalchemy.sql.expression import literal_column

FLAGS = flags.FLAGS

LOG = logging.getLogger(__name__)


def is_admin_context(context):
    """Indicates if the request context is an administrator."""
    if not context:
        warnings.warn(_('Use of empty request context is deprecated'),
                      DeprecationWarning)
        raise Exception('die')
    return context.is_admin


def is_user_context(context):
    """Indicates if the request context is a normal user."""
    if not context:
        return False
    if context.is_admin:
        return False
    if not context.user_id or not context.project_id:
        return False
    return True


def authorize_project_context(context, project_id):
    """Ensures a request has permission to access the given project."""
    if is_user_context(context):
        if not context.project_id:
            raise exception.NotAuthorized()
        elif context.project_id != project_id:
            raise exception.NotAuthorized()


def authorize_user_context(context, user_id):
    """Ensures a request has permission to access the given user."""
    if is_user_context(context):
        if not context.user_id:
            raise exception.NotAuthorized()
        elif context.user_id != user_id:
            raise exception.NotAuthorized()


def authorize_quota_class_context(context, class_name):
    """Ensures a request has permission to access the given quota class."""
    if is_user_context(context):
        if not context.quota_class:
            raise exception.NotAuthorized()
        elif context.quota_class != class_name:
            raise exception.NotAuthorized()


def require_admin_context(f):
    """Decorator to require admin request context.

    The first argument to the wrapped function must be the context.

    """

    def wrapper(*args, **kwargs):
        if not is_admin_context(args[0]):
            raise exception.AdminRequired()
        return f(*args, **kwargs)
    return wrapper


def require_context(f):
    """Decorator to require *any* user or admin context.

    This does no authorization for user or project access matching, see
    :py:func:`authorize_project_context` and
    :py:func:`authorize_user_context`.

    The first argument to the wrapped function must be the context.

    """

    def wrapper(*args, **kwargs):
        if not is_admin_context(args[0]) and not is_user_context(args[0]):
            raise exception.NotAuthorized()
        return f(*args, **kwargs)
    return wrapper


def require_volume_exists(f):
    """Decorator to require the specified volume to exist.

    Requires the wrapped function to use context and volume_id as
    their first two arguments.
    """

    def wrapper(context, volume_id, *args, **kwargs):
        db.volume_get(context, volume_id)
        return f(context, volume_id, *args, **kwargs)
    wrapper.__name__ = f.__name__
    return wrapper


def model_query(context, *args, **kwargs):
    """Query helper that accounts for context's `read_deleted` field.

    :param context: context to query under
    :param session: if present, the session to use
    :param read_deleted: if present, overrides context's read_deleted field.
    :param project_only: if present and context is user-type, then restrict
            query to match the context's project_id.
    """
    session = kwargs.get('session') or get_session()
    read_deleted = kwargs.get('read_deleted') or context.read_deleted
    project_only = kwargs.get('project_only')

    query = session.query(*args)

    if read_deleted == 'no':
        query = query.filter_by(deleted=False)
    elif read_deleted == 'yes':
        pass  # omit the filter to include deleted and active
    elif read_deleted == 'only':
        query = query.filter_by(deleted=True)
    else:
        raise Exception(
                _("Unrecognized read_deleted value '%s'") % read_deleted)

    if project_only and is_user_context(context):
        query = query.filter_by(project_id=context.project_id)

    return query


def exact_filter(query, model, filters, legal_keys):
    """Applies exact match filtering to a query.

    Returns the updated query.  Modifies filters argument to remove
    filters consumed.

    :param query: query to apply filters to
    :param model: model object the query applies to, for IN-style
                  filtering
    :param filters: dictionary of filters; values that are lists,
                    tuples, sets, or frozensets cause an 'IN' test to
                    be performed, while exact matching ('==' operator)
                    is used for other values
    :param legal_keys: list of keys to apply exact filtering to
    """

    filter_dict = {}

    # Walk through all the keys
    for key in legal_keys:
        # Skip ones we're not filtering on
        if key not in filters:
            continue

        # OK, filtering on this key; what value do we search for?
        value = filters.pop(key)

        if isinstance(value, (list, tuple, set, frozenset)):
            # Looking for values in a list; apply to query directly
            column_attr = getattr(model, key)
            query = query.filter(column_attr.in_(value))
        else:
            # OK, simple exact match; save for later
            filter_dict[key] = value

    # Apply simple exact matches
    if filter_dict:
        query = query.filter_by(**filter_dict)

    return query


###################


@require_admin_context
def service_destroy(context, service_id):
    session = get_session()
    with session.begin():
        service_ref = service_get(context, service_id, session=session)
        service_ref.delete(session=session)


@require_admin_context
def service_get(context, service_id, session=None):
    result = model_query(context, models.Service, session=session).\
                     filter_by(id=service_id).\
                     first()
    if not result:
        raise exception.ServiceNotFound(service_id=service_id)

    return result


@require_admin_context
def service_get_all(context, disabled=None):
    query = model_query(context, models.Service)

    if disabled is not None:
        query = query.filter_by(disabled=disabled)

    return query.all()


@require_admin_context
def service_get_all_by_topic(context, topic):
    return model_query(context, models.Service, read_deleted="no").\
                filter_by(disabled=False).\
                filter_by(topic=topic).\
                all()


@require_admin_context
def service_get_by_host_and_topic(context, host, topic):
    return model_query(context, models.Service, read_deleted="no").\
                filter_by(disabled=False).\
                filter_by(host=host).\
                filter_by(topic=topic).\
                first()


@require_admin_context
def service_get_all_by_host(context, host):
    return model_query(context, models.Service, read_deleted="no").\
                filter_by(host=host).\
                all()


@require_admin_context
def _service_get_all_topic_subquery(context, session, topic, subq, label):
    sort_value = getattr(subq.c, label)
    return model_query(context, models.Service,
                       func.coalesce(sort_value, 0),
                       session=session, read_deleted="no").\
                filter_by(topic=topic).\
                filter_by(disabled=False).\
                outerjoin((subq, models.Service.host == subq.c.host)).\
                order_by(sort_value).\
                all()


@require_admin_context
def service_get_all_volume_sorted(context):
    session = get_session()
    with session.begin():
        topic = FLAGS.volume_topic
        label = 'volume_gigabytes'
        subq = model_query(context, models.Volume.host,
                           func.sum(models.Volume.size).label(label),
                           session=session, read_deleted="no").\
                       group_by(models.Volume.host).\
                       subquery()
        return _service_get_all_topic_subquery(context,
                                               session,
                                               topic,
                                               subq,
                                               label)


@require_admin_context
def service_get_by_args(context, host, binary):
    result = model_query(context, models.Service).\
                     filter_by(host=host).\
                     filter_by(binary=binary).\
                     first()

    if not result:
        raise exception.HostBinaryNotFound(host=host, binary=binary)

    return result


@require_admin_context
def service_create(context, values):
    service_ref = models.Service()
    service_ref.update(values)
    if not FLAGS.enable_new_services:
        service_ref.disabled = True
    service_ref.save()
    return service_ref


@require_admin_context
def service_update(context, service_id, values):
    session = get_session()
    with session.begin():
        service_ref = service_get(context, service_id, session=session)
        service_ref.update(values)
        service_ref.save(session=session)


###################


def _metadata_refs(metadata_dict, meta_class):
    metadata_refs = []
    if metadata_dict:
        for k, v in metadata_dict.iteritems():
            metadata_ref = meta_class()
            metadata_ref['key'] = k
            metadata_ref['value'] = v
            metadata_refs.append(metadata_ref)
    return metadata_refs


def _dict_with_extra_specs(inst_type_query):
    """Takes an instance, volume, or instance type query returned
    by sqlalchemy and returns it as a dictionary, converting the
    extra_specs entry from a list of dicts:

    'extra_specs' : [{'key': 'k1', 'value': 'v1', ...}, ...]

    to a single dict:

    'extra_specs' : {'k1': 'v1'}

    """
    inst_type_dict = dict(inst_type_query)
    extra_specs = dict([(x['key'], x['value'])
                        for x in inst_type_query['extra_specs']])
    inst_type_dict['extra_specs'] = extra_specs
    return inst_type_dict


###################


@require_admin_context
def iscsi_target_count_by_host(context, host):
    return model_query(context, models.IscsiTarget).\
                   filter_by(host=host).\
                   count()


@require_admin_context
def iscsi_target_create_safe(context, values):
    iscsi_target_ref = models.IscsiTarget()

    for (key, value) in values.iteritems():
        iscsi_target_ref[key] = value
    try:
        iscsi_target_ref.save()
        return iscsi_target_ref
    except IntegrityError:
        return None


###################


@require_admin_context
def volume_allocate_iscsi_target(context, volume_id, host):
    session = get_session()
    with session.begin():
        iscsi_target_ref = model_query(context, models.IscsiTarget,
                                       session=session, read_deleted="no").\
                                filter_by(volume=None).\
                                filter_by(host=host).\
                                with_lockmode('update').\
                                first()

        # NOTE(vish): if with_lockmode isn't supported, as in sqlite,
        #             then this has concurrency issues
        if not iscsi_target_ref:
            raise db.NoMoreTargets()

        iscsi_target_ref.volume_id = volume_id
        session.add(iscsi_target_ref)

    return iscsi_target_ref.target_num


@require_admin_context
def volume_attached(context, volume_id, instance_uuid, mountpoint):
    if not utils.is_uuid_like(instance_uuid):
        raise exception.InvalidUUID(instance_uuid)

    session = get_session()
    with session.begin():
        volume_ref = volume_get(context, volume_id, session=session)
        volume_ref['status'] = 'in-use'
        volume_ref['mountpoint'] = mountpoint
        volume_ref['attach_status'] = 'attached'
        volume_ref['instance_uuid'] = instance_uuid
        volume_ref.save(session=session)


@require_context
def volume_create(context, values):
    values['volume_metadata'] = _metadata_refs(values.get('metadata'),
                                               models.VolumeMetadata)
    volume_ref = models.Volume()
    if not values.get('id'):
        values['id'] = str(utils.gen_uuid())
    volume_ref.update(values)

    session = get_session()
    with session.begin():
        volume_ref.save(session=session)

    meta = volume_metadata_get(context, volume_ref.id)
    volume_ref.metadata = meta

    result = model_query(context, models.Volume, read_deleted="no").\
                         options(joinedload('volume_metadata')).\
                         filter_by(id=volume_ref['id']).first()
    if not result:
        raise exception.VolumeNotFound(volume_id=volume_ref['id'])

    return result


@require_admin_context
def volume_data_get_for_project(context, project_id):
    result = model_query(context,
                         func.count(models.Volume.id),
                         func.sum(models.Volume.size),
                         read_deleted="no").\
                     filter_by(project_id=project_id).\
                     first()

    # NOTE(vish): convert None to 0
    return (result[0] or 0, result[1] or 0)


@require_admin_context
def volume_destroy(context, volume_id):
    session = get_session()
    with session.begin():
        session.query(models.Volume).\
                filter_by(id=volume_id).\
                update({'deleted': True,
                        'deleted_at': timeutils.utcnow(),
                        'updated_at': literal_column('updated_at')})
        session.query(models.IscsiTarget).\
                filter_by(volume_id=volume_id).\
                update({'volume_id': None})
        session.query(models.VolumeMetadata).\
                filter_by(volume_id=volume_id).\
                update({'deleted': True,
                        'deleted_at': timeutils.utcnow(),
                        'updated_at': literal_column('updated_at')})


@require_admin_context
def volume_detached(context, volume_id):
    session = get_session()
    with session.begin():
        volume_ref = volume_get(context, volume_id, session=session)
        volume_ref['status'] = 'available'
        volume_ref['mountpoint'] = None
        volume_ref['attach_status'] = 'detached'
        volume_ref['instance_uuid'] = None
        volume_ref.save(session=session)


@require_context
def _volume_get_query(context, session=None, project_only=False):
    return model_query(context, models.Volume, session=session,
                       project_only=project_only).\
                       options(joinedload('volume_metadata')).\
                       options(joinedload('volume_type'))


@require_context
def _ec2_volume_get_query(context, session=None, project_only=False):
    return model_query(context, models.VolumeIdMapping, session=session,
                       project_only=project_only)


@require_context
def _ec2_snapshot_get_query(context, session=None, project_only=False):
    return model_query(context, models.SnapshotIdMapping, session=session,
                       project_only=project_only)


@require_context
def volume_get(context, volume_id, session=None):
    result = _volume_get_query(context, session=session, project_only=True).\
                    filter_by(id=volume_id).\
                    first()

    if not result:
        raise exception.VolumeNotFound(volume_id=volume_id)

    return result


@require_admin_context
def volume_get_all(context):
    return _volume_get_query(context).all()


@require_admin_context
def volume_get_all_by_host(context, host):
    return _volume_get_query(context).filter_by(host=host).all()


@require_admin_context
def volume_get_all_by_instance_uuid(context, instance_uuid):
    result = model_query(context, models.Volume, read_deleted="no").\
                     options(joinedload('volume_metadata')).\
                     options(joinedload('volume_type')).\
                     filter_by(instance_uuid=instance_uuid).\
                     all()

    if not result:
        return []

    return result


@require_context
def volume_get_all_by_project(context, project_id):
    authorize_project_context(context, project_id)
    return _volume_get_query(context).filter_by(project_id=project_id).all()


@require_admin_context
def volume_get_iscsi_target_num(context, volume_id):
    result = model_query(context, models.IscsiTarget, read_deleted="yes").\
                     filter_by(volume_id=volume_id).\
                     first()

    if not result:
        raise exception.ISCSITargetNotFoundForVolume(volume_id=volume_id)

    return result.target_num


@require_context
def volume_update(context, volume_id, values):
    session = get_session()
    metadata = values.get('metadata')
    if metadata is not None:
        volume_metadata_update(context,
                                volume_id,
                                values.pop('metadata'),
                                delete=True)
    with session.begin():
        volume_ref = volume_get(context, volume_id, session=session)
        volume_ref.update(values)
        volume_ref.save(session=session)


####################

def _volume_metadata_get_query(context, volume_id, session=None):
    return model_query(context, models.VolumeMetadata,
                       session=session, read_deleted="no").\
                    filter_by(volume_id=volume_id)


@require_context
@require_volume_exists
def volume_metadata_get(context, volume_id):
    rows = _volume_metadata_get_query(context, volume_id).all()
    result = {}
    for row in rows:
        result[row['key']] = row['value']

    return result


@require_context
@require_volume_exists
def volume_metadata_delete(context, volume_id, key):
    _volume_metadata_get_query(context, volume_id).\
        filter_by(key=key).\
        update({'deleted': True,
                'deleted_at': timeutils.utcnow(),
                'updated_at': literal_column('updated_at')})


@require_context
@require_volume_exists
def volume_metadata_get_item(context, volume_id, key, session=None):
    result = _volume_metadata_get_query(context, volume_id, session=session).\
                    filter_by(key=key).\
                    first()

    if not result:
        raise exception.VolumeMetadataNotFound(metadata_key=key,
                                               volume_id=volume_id)
    return result


@require_context
@require_volume_exists
def volume_metadata_update(context, volume_id, metadata, delete):
    session = get_session()

    # Set existing metadata to deleted if delete argument is True
    if delete:
        original_metadata = volume_metadata_get(context, volume_id)
        for meta_key, meta_value in original_metadata.iteritems():
            if meta_key not in metadata:
                meta_ref = volume_metadata_get_item(context, volume_id,
                                                    meta_key, session)
                meta_ref.update({'deleted': True})
                meta_ref.save(session=session)

    meta_ref = None

    # Now update all existing items with new values, or create new meta objects
    for meta_key, meta_value in metadata.iteritems():

        # update the value whether it exists or not
        item = {"value": meta_value}

        try:
            meta_ref = volume_metadata_get_item(context, volume_id,
                                                  meta_key, session)
        except exception.VolumeMetadataNotFound, e:
            meta_ref = models.VolumeMetadata()
            item.update({"key": meta_key, "volume_id": volume_id})

        meta_ref.update(item)
        meta_ref.save(session=session)

    return metadata


###################


@require_context
def snapshot_create(context, values):
    snapshot_ref = models.Snapshot()
    if not values.get('id'):
        values['id'] = str(utils.gen_uuid())
    snapshot_ref.update(values)

    session = get_session()
    with session.begin():
        snapshot_ref.save(session=session)
    return snapshot_ref


@require_admin_context
def snapshot_destroy(context, snapshot_id):
    session = get_session()
    with session.begin():
        session.query(models.Snapshot).\
                filter_by(id=snapshot_id).\
                update({'deleted': True,
                        'deleted_at': timeutils.utcnow(),
                        'updated_at': literal_column('updated_at')})


@require_context
def snapshot_get(context, snapshot_id, session=None):
    result = model_query(context, models.Snapshot, session=session,
                         project_only=True).\
                filter_by(id=snapshot_id).\
                first()

    if not result:
        raise exception.SnapshotNotFound(snapshot_id=snapshot_id)

    return result


@require_admin_context
def snapshot_get_all(context):
    return model_query(context, models.Snapshot).all()


@require_context
def snapshot_get_all_for_volume(context, volume_id):
    return model_query(context, models.Snapshot, read_deleted='no',
                       project_only=True).\
              filter_by(volume_id=volume_id).all()


@require_context
def snapshot_get_all_by_project(context, project_id):
    authorize_project_context(context, project_id)
    return model_query(context, models.Snapshot).\
                   filter_by(project_id=project_id).\
                   all()


@require_context
def snapshot_update(context, snapshot_id, values):
    session = get_session()
    with session.begin():
        snapshot_ref = snapshot_get(context, snapshot_id, session=session)
        snapshot_ref.update(values)
        snapshot_ref.save(session=session)


###################


@require_admin_context
def migration_create(context, values):
    migration = models.Migration()
    migration.update(values)
    migration.save()
    return migration


@require_admin_context
def migration_update(context, id, values):
    session = get_session()
    with session.begin():
        migration = migration_get(context, id, session=session)
        migration.update(values)
        migration.save(session=session)
        return migration


@require_admin_context
def migration_get(context, id, session=None):
    result = model_query(context, models.Migration, session=session,
                         read_deleted="yes").\
                     filter_by(id=id).\
                     first()

    if not result:
        raise exception.MigrationNotFound(migration_id=id)

    return result


@require_admin_context
def migration_get_by_instance_and_status(context, instance_uuid, status):
    result = model_query(context, models.Migration, read_deleted="yes").\
                     filter_by(instance_uuid=instance_uuid).\
                     filter_by(status=status).\
                     first()

    if not result:
        raise exception.MigrationNotFoundByStatus(instance_id=instance_uuid,
                                                  status=status)

    return result


@require_admin_context
def migration_get_all_unconfirmed(context, confirm_window, session=None):
    confirm_window = timeutils.utcnow() - datetime.timedelta(
            seconds=confirm_window)

    return model_query(context, models.Migration, session=session,
                       read_deleted="yes").\
            filter(models.Migration.updated_at <= confirm_window).\
            filter_by(status="finished").\
            all()


##################


@require_admin_context
def volume_type_create(context, values):
    """Create a new instance type. In order to pass in extra specs,
    the values dict should contain a 'extra_specs' key/value pair:

    {'extra_specs' : {'k1': 'v1', 'k2': 'v2', ...}}

    """
    session = get_session()
    with session.begin():
        try:
            volume_type_get_by_name(context, values['name'], session)
            raise exception.VolumeTypeExists(name=values['name'])
        except exception.VolumeTypeNotFoundByName:
            pass
        try:
            values['extra_specs'] = _metadata_refs(values.get('extra_specs'),
                                                   models.VolumeTypeExtraSpecs)
            volume_type_ref = models.VolumeTypes()
            volume_type_ref.update(values)
            volume_type_ref.save()
        except Exception, e:
            raise exception.DBError(e)
        return volume_type_ref


@require_context
def volume_type_get_all(context, inactive=False, filters=None):
    """
    Returns a dict describing all volume_types with name as key.
    """
    filters = filters or {}

    read_deleted = "yes" if inactive else "no"
    rows = model_query(context, models.VolumeTypes,
                       read_deleted=read_deleted).\
                        options(joinedload('extra_specs')).\
                        order_by("name").\
                        all()

    # TODO(sirp): this patern of converting rows to a result with extra_specs
    # is repeated quite a bit, might be worth creating a method for it
    result = {}
    for row in rows:
        result[row['name']] = _dict_with_extra_specs(row)

    return result


@require_context
def volume_type_get(context, id, session=None):
    """Returns a dict describing specific volume_type"""
    result = model_query(context, models.VolumeTypes, session=session).\
                    options(joinedload('extra_specs')).\
                    filter_by(id=id).\
                    first()

    if not result:
        raise exception.VolumeTypeNotFound(volume_type_id=id)

    return _dict_with_extra_specs(result)


@require_context
def volume_type_get_by_name(context, name, session=None):
    """Returns a dict describing specific volume_type"""
    result = model_query(context, models.VolumeTypes, session=session).\
                    options(joinedload('extra_specs')).\
                    filter_by(name=name).\
                    first()

    if not result:
        raise exception.VolumeTypeNotFoundByName(volume_type_name=name)
    else:
        return _dict_with_extra_specs(result)


@require_admin_context
def volume_type_destroy(context, name):
    session = get_session()
    with session.begin():
        volume_type_ref = volume_type_get_by_name(context, name,
                                                  session=session)
        volume_type_id = volume_type_ref['id']
        session.query(models.VolumeTypes).\
                filter_by(id=volume_type_id).\
                update({'deleted': True,
                        'deleted_at': timeutils.utcnow(),
                        'updated_at': literal_column('updated_at')})
        session.query(models.VolumeTypeExtraSpecs).\
                filter_by(volume_type_id=volume_type_id).\
                update({'deleted': True,
                        'deleted_at': timeutils.utcnow(),
                        'updated_at': literal_column('updated_at')})


@require_context
def volume_get_active_by_window(context, begin, end=None,
                                         project_id=None):
    """Return volumes that were active during window."""
    session = get_session()
    query = session.query(models.Volume)

    query = query.filter(or_(models.Volume.deleted_at == None,
                             models.Volume.deleted_at > begin))
    if end:
        query = query.filter(models.Volume.created_at < end)
    if project_id:
        query = query.filter_by(project_id=project_id)

    return query.all()


####################


def _volume_type_extra_specs_query(context, volume_type_id, session=None):
    return model_query(context, models.VolumeTypeExtraSpecs, session=session,
                       read_deleted="no").\
                    filter_by(volume_type_id=volume_type_id)


@require_context
def volume_type_extra_specs_get(context, volume_type_id):
    rows = _volume_type_extra_specs_query(context, volume_type_id).\
                    all()

    result = {}
    for row in rows:
        result[row['key']] = row['value']

    return result


@require_context
def volume_type_extra_specs_delete(context, volume_type_id, key):
    _volume_type_extra_specs_query(context, volume_type_id).\
        filter_by(key=key).\
        update({'deleted': True,
                'deleted_at': timeutils.utcnow(),
                'updated_at': literal_column('updated_at')})


@require_context
def volume_type_extra_specs_get_item(context, volume_type_id, key,
                                     session=None):
    result = _volume_type_extra_specs_query(
                                    context, volume_type_id, session=session).\
                    filter_by(key=key).\
                    first()

    if not result:
        raise exception.VolumeTypeExtraSpecsNotFound(
                   extra_specs_key=key, volume_type_id=volume_type_id)

    return result


@require_context
def volume_type_extra_specs_update_or_create(context, volume_type_id,
                                             specs):
    session = get_session()
    spec_ref = None
    for key, value in specs.iteritems():
        try:
            spec_ref = volume_type_extra_specs_get_item(
                context, volume_type_id, key, session)
        except exception.VolumeTypeExtraSpecsNotFound, e:
            spec_ref = models.VolumeTypeExtraSpecs()
        spec_ref.update({"key": key, "value": value,
                         "volume_type_id": volume_type_id,
                         "deleted": 0})
        spec_ref.save(session=session)
    return specs


####################


@require_admin_context
def sm_backend_conf_create(context, values):
    backend_conf = models.SMBackendConf()
    backend_conf.update(values)
    backend_conf.save()
    return backend_conf


@require_admin_context
def sm_backend_conf_update(context, sm_backend_id, values):
    session = get_session()
    with session.begin():
        backend_conf = model_query(context, models.SMBackendConf,
                                   session=session,
                                   read_deleted="yes").\
                           filter_by(id=sm_backend_id).\
                           first()

        if not backend_conf:
            raise exception.NotFound(
                _("No backend config with id %(sm_backend_id)s") % locals())

        backend_conf.update(values)
        backend_conf.save(session=session)
    return backend_conf


@require_admin_context
def sm_backend_conf_delete(context, sm_backend_id):
    # FIXME(sirp): for consistency, shouldn't this just mark as deleted with
    # `purge` actually deleting the record?
    session = get_session()
    with session.begin():
        model_query(context, models.SMBackendConf, session=session,
                    read_deleted="yes").\
                filter_by(id=sm_backend_id).\
                delete()


@require_admin_context
def sm_backend_conf_get(context, sm_backend_id):
    result = model_query(context, models.SMBackendConf, read_deleted="yes").\
                     filter_by(id=sm_backend_id).\
                     first()

    if not result:
        raise exception.NotFound(_("No backend config with id "
                                   "%(sm_backend_id)s") % locals())

    return result


@require_admin_context
def sm_backend_conf_get_by_sr(context, sr_uuid):
    return model_query(context, models.SMBackendConf, read_deleted="yes").\
                    filter_by(sr_uuid=sr_uuid).\
                    first()


@require_admin_context
def sm_backend_conf_get_all(context):
    return model_query(context, models.SMBackendConf, read_deleted="yes").\
                    all()


####################


def _sm_flavor_get_query(context, sm_flavor_label, session=None):
    return model_query(context, models.SMFlavors, session=session,
                       read_deleted="yes").\
                        filter_by(label=sm_flavor_label)


@require_admin_context
def sm_flavor_create(context, values):
    sm_flavor = models.SMFlavors()
    sm_flavor.update(values)
    sm_flavor.save()
    return sm_flavor


@require_admin_context
def sm_flavor_update(context, sm_flavor_label, values):
    sm_flavor = sm_flavor_get(context, sm_flavor_label)
    sm_flavor.update(values)
    sm_flavor.save()
    return sm_flavor


@require_admin_context
def sm_flavor_delete(context, sm_flavor_label):
    session = get_session()
    with session.begin():
        _sm_flavor_get_query(context, sm_flavor_label).delete()


@require_admin_context
def sm_flavor_get(context, sm_flavor_label):
    result = _sm_flavor_get_query(context, sm_flavor_label).first()

    if not result:
        raise exception.NotFound(
                _("No sm_flavor called %(sm_flavor)s") % locals())

    return result


@require_admin_context
def sm_flavor_get_all(context):
    return model_query(context, models.SMFlavors, read_deleted="yes").all()


###############################


def _sm_volume_get_query(context, volume_id, session=None):
    return model_query(context, models.SMVolume, session=session,
                       read_deleted="yes").\
                        filter_by(id=volume_id)


def sm_volume_create(context, values):
    sm_volume = models.SMVolume()
    sm_volume.update(values)
    sm_volume.save()
    return sm_volume


def sm_volume_update(context, volume_id, values):
    sm_volume = sm_volume_get(context, volume_id)
    sm_volume.update(values)
    sm_volume.save()
    return sm_volume


def sm_volume_delete(context, volume_id):
    session = get_session()
    with session.begin():
        _sm_volume_get_query(context, volume_id, session=session).delete()


def sm_volume_get(context, volume_id):
    result = _sm_volume_get_query(context, volume_id).first()

    if not result:
        raise exception.NotFound(
                _("No sm_volume with id %(volume_id)s") % locals())

    return result


def sm_volume_get_all(context):
    return model_query(context, models.SMVolume, read_deleted="yes").all()


###############################


@require_context
def quota_get(context, project_id, resource, session=None):
    result = model_query(context, models.Quota, session=session,
                         read_deleted="no").\
                     filter_by(project_id=project_id).\
                     filter_by(resource=resource).\
                     first()

    if not result:
        raise exception.ProjectQuotaNotFound(project_id=project_id)

    return result


@require_context
def quota_get_all_by_project(context, project_id):
    authorize_project_context(context, project_id)

    rows = model_query(context, models.Quota, read_deleted="no").\
                   filter_by(project_id=project_id).\
                   all()

    result = {'project_id': project_id}
    for row in rows:
        result[row.resource] = row.hard_limit

    return result


@require_admin_context
def quota_create(context, project_id, resource, limit):
    quota_ref = models.Quota()
    quota_ref.project_id = project_id
    quota_ref.resource = resource
    quota_ref.hard_limit = limit
    quota_ref.save()
    return quota_ref


@require_admin_context
def quota_update(context, project_id, resource, limit):
    session = get_session()
    with session.begin():
        quota_ref = quota_get(context, project_id, resource, session=session)
        quota_ref.hard_limit = limit
        quota_ref.save(session=session)


@require_admin_context
def quota_destroy(context, project_id, resource):
    session = get_session()
    with session.begin():
        quota_ref = quota_get(context, project_id, resource, session=session)
        quota_ref.delete(session=session)


@require_admin_context
def quota_destroy_all_by_project(context, project_id):
    session = get_session()
    with session.begin():
        quotas = model_query(context, models.Quota, session=session,
                             read_deleted="no").\
                         filter_by(project_id=project_id).\
                         all()

        for quota_ref in quotas:
            quota_ref.delete(session=session)


###################


@require_context
def quota_class_get(context, class_name, resource, session=None):
    result = model_query(context, models.QuotaClass, session=session,
                         read_deleted="no").\
                     filter_by(class_name=class_name).\
                     filter_by(resource=resource).\
                     first()

    if not result:
        raise exception.QuotaClassNotFound(class_name=class_name)

    return result


@require_context
def quota_class_get_all_by_name(context, class_name):
    authorize_quota_class_context(context, class_name)

    rows = model_query(context, models.QuotaClass, read_deleted="no").\
                   filter_by(class_name=class_name).\
                   all()

    result = {'class_name': class_name}
    for row in rows:
        result[row.resource] = row.hard_limit

    return result


@require_admin_context
def quota_class_create(context, class_name, resource, limit):
    quota_class_ref = models.QuotaClass()
    quota_class_ref.class_name = class_name
    quota_class_ref.resource = resource
    quota_class_ref.hard_limit = limit
    quota_class_ref.save()
    return quota_class_ref


@require_admin_context
def quota_class_update(context, class_name, resource, limit):
    session = get_session()
    with session.begin():
        quota_class_ref = quota_class_get(context, class_name, resource,
                                          session=session)
        quota_class_ref.hard_limit = limit
        quota_class_ref.save(session=session)


@require_admin_context
def quota_class_destroy(context, class_name, resource):
    session = get_session()
    with session.begin():
        quota_class_ref = quota_class_get(context, class_name, resource,
                                          session=session)
        quota_class_ref.delete(session=session)


@require_admin_context
def quota_class_destroy_all_by_name(context, class_name):
    session = get_session()
    with session.begin():
        quota_classes = model_query(context, models.QuotaClass,
                                    session=session, read_deleted="no").\
                                filter_by(class_name=class_name).\
                                all()

        for quota_class_ref in quota_classes:
            quota_class_ref.delete(session=session)


@require_context
def quota_usage_get(context, project_id, resource, session=None):
    result = model_query(context, models.QuotaUsage, session=session,
                         read_deleted="no").\
                     filter_by(project_id=project_id).\
                     filter_by(resource=resource).\
                     first()

    if not result:
        raise exception.QuotaUsageNotFound(project_id=project_id)

    return result


@require_context
def quota_usage_get_all_by_project(context, project_id):
    authorize_project_context(context, project_id)

    rows = model_query(context, models.QuotaUsage, read_deleted="no").\
                   filter_by(project_id=project_id).\
                   all()

    result = {'project_id': project_id}
    for row in rows:
        result[row.resource] = dict(in_use=row.in_use, reserved=row.reserved)

    return result


@require_admin_context
def quota_usage_create(context, project_id, resource, in_use, reserved,
                       until_refresh, session=None, save=True):
    quota_usage_ref = models.QuotaUsage()
    quota_usage_ref.project_id = project_id
    quota_usage_ref.resource = resource
    quota_usage_ref.in_use = in_use
    quota_usage_ref.reserved = reserved
    quota_usage_ref.until_refresh = until_refresh

    # Allow us to hold the save operation until later; keeps the
    # transaction in quota_reserve() from breaking too early
    if save:
        quota_usage_ref.save(session=session)

    return quota_usage_ref


@require_admin_context
def quota_usage_update(context, project_id, resource, in_use, reserved,
                       until_refresh, session=None):
    def do_update(session):
        quota_usage_ref = quota_usage_get(context, project_id, resource,
                                          session=session)
        quota_usage_ref.in_use = in_use
        quota_usage_ref.reserved = reserved
        quota_usage_ref.until_refresh = until_refresh
        quota_usage_ref.save(session=session)

    if session:
        # Assume caller started a transaction
        do_update(session)
    else:
        session = get_session()
        with session.begin():
            do_update(session)


@require_admin_context
def quota_usage_destroy(context, project_id, resource):
    session = get_session()
    with session.begin():
        quota_usage_ref = quota_usage_get(context, project_id, resource,
                                          session=session)
        quota_usage_ref.delete(session=session)


################


def _share_get_query(context, session=None):
    if session is None:
        session = get_session()
    query = model_query(context, models.Share, session=session)
    return query.filter_by(deleted=False)


def _share_volume_get_query(context, session=None):
    if session is None:
        session = get_session()
    query = model_query(context, models.Share, models.Volume,
                        session=session).join(models.Volume)
    return query.filter_by(deleted=False)


@require_context
def share_create(context, values):
    share_ref = models.Share()
    share_ref.update(values)
    session = get_session()
    with session.begin():
        share_ref.save(session=session)

    return share_ref


@require_context
def share_update(context, share_id, values):
    session = get_session()
    with session.begin():
        share_ref = share_get(context, share_id, session=session)
        share_ref.update(values)
        share_ref.save(session=session)


@require_context
def share_get(context, share_id, session=None):
    result = _share_get_query(context, session).filter_by(id=share_id).first()
    if result is None:
        raise exception.NotFound()
    return result


@require_context
def share_volume_get(context, volume_id):
    result = _share_volume_get_query(context)
    result = result.filter_by(id=volume_id).first()
    if result is None:
        raise exception.NotFound()
    return result


@require_admin_context
def share_volume_get_all(context):
    return _share_volume_get_query(context).all()


@require_admin_context
def shares_volume_get_all_by_host(context, host):
    query = _share_volume_get_query(context)
    return query.filter_by(host=host).all()


@require_context
def share_volume_get_all_by_project(context, project_id):
    """Returns list of (Share, Volume) pairs with given project ID"""
    return _share_volume_get_query(context).\
                filter_by(project_id=project_id).all()


@require_context
def share_get_by_volume_id(context, volume_id):
    result = _share_get_query(context).filter_by(volume_id=volume_id)
    result = result.first()
    if result is None:
        raise exception.NotFound()
    return result


@require_context
def share_delete(context, share_id):
    session = get_session()
    share_ref = share_get(context, share_id, session)
    share_ref.update({'deleted': True,
                      'deleted_at': timeutils.utcnow(),
                      'updated_at': literal_column('updated_at'),
                      'status': 'deleted'})
    share_ref.save(session)


###################


def _share_access_get_query(context, session, values):
    """
    Get access record
    """
    query = model_query(context, models.ShareAccessMapping, session=session)
    return query.filter_by(**values)


@require_context
def share_access_create(context, values):
    session = get_session()
    with session.begin():
        access_ref = models.ShareAccessMapping()
        access_ref.update(values)
        access_ref.save(session=session)
        return access_ref


@require_context
def share_access_get(context, access_id):
    """
    Get access record
    """
    session = get_session()
    access = _share_access_get_query(context, session,
                                     {'id': access_id}).first()
    if access:
        return access
    else:
        raise exception.NotFound()


@require_context
def share_access_get_all_for_share(context, volume_id):
    session = get_session()
    return _share_access_get_query(context, session,
                                   {'volume_id': volume_id}).all()


@require_context
def share_access_delete(context, access_id):
    session = get_session()
    with session.begin():
        session.query(models.ShareAccessMapping).\
        filter_by(id=access_id).\
        update({'deleted': True,
                'deleted_at': timeutils.utcnow(),
                'updated_at': literal_column('updated_at'),
                'state': models.ShareAccessMapping.STATE_DELETED})


@require_context
def share_access_update(context, access_id, values):
    session = get_session()
    with session.begin():
        access = _share_access_get_query(context, session, {'id': access_id})
        access = access.one()
        access.update(values)
        access.save(session=session)
