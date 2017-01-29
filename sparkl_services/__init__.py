"""
Copyright 2016 Sparkl Limited. All Rights Reserved.
Authors: Andrew Farrell <ahfarrell@sparkl.com>
Library for services to handle communication with SPARKL.
A service register itself with handlers for
particular events and then proceeds to start the service.
"""

import logging
import json
from sparkl_script import executeresults, execute_as_python

logger = logging.getLogger(__name__)

# callbacks
callbacks = {}

# handle for comms
handle = None

s_undefined = "undefined"

# service related string literals
s_service = 'service'
s_type = 'type'
s_provision = 'provision'
s_type_containerservice = 'docker'
s_type_scriptservice = 'script'
s_prop = 'prop'
s_prop_dockerconfig = 'docker.config'
s_docker_src = 'docker.src'
s_script_src = 'script.src'
s_envsdir = "envs"
##

# meta-data state
s_md_optags = "optags"
s_md_opreplies = "opreplies"
s_md_opids = "opids"
s_md_opnames = "opnames"
s_md_fieldids = "fieldids"
s_md_fieldnames = "fieldnames"
s_md_props = "props"
s_md_type = s_type
metadata = {
}
##

# event types
et_open = 'open_event'
et_aggevent = 'aggregate_event'
et_dataevent = 'data_event'
et_errorevent = 'error_event'
et_internal = 'internal'
##

# event record strings
s_tag = 'tag'
s_content = 'content'
s_attributes = 'attr'
s_instanceid = 'ref'
s_name = 'name'
s_id = 'id'
s_fieldtag = 'field'
s_datumtag = 'datum'
s_fieldstag = 'fields'
s_fielddata = 'data'
s_subject = 'subject'
s_opid = s_subject
s_ref = 'ref'
s_reason = 'reason'
s_error = 'error'
##

# operation types in meta-data
s_op_rr = 'request'
s_op_rrs = s_op_rr + 's'
s_op_sr = "solict"
s_op_srs = s_op_sr + 's'
s_op_co = 'consume'
s_op_cos = s_op_co + 's'
s_op_ow = 'one_way'
s_op_no = 'notify'
s_op_nos = 'notifies'
s_op_re = 'response'
s_op_res = s_op_re + 's'
##

# reply related strings
s_outputname = 'outputName'
s_output = 'fields'
s_replytag = 'reply'
s_replyTo = 'replyTo'
s_msgid = '_msgid'
##

s_main_op = 'main'


def register_eventcallback(eventtype, callback):
    """
    Register call-back for an event type.
    The library will call this with an event tuple when an event of the given
    type is received.

    :type: str
    :param eventtype: the event type - see event type strings above

    :type: func
    :param callback: the function to call back in receipt of an event of the
    given type
    """
    logger.debug('registering callback ' + str(callback) + ' for '+eventtype)
    callbacks[eventtype] = callback


def handle_msg(msg_, envpath):
    """
    Handles JSON message received on message transport.
    Returns a tuple indicating success and a yielded reply.

    :type: str
    :param msg_: Message received.

    :rtype: bool, dict
    :return: whether success and yielded reply

    :type: str
    :param envpath: location of instance env directory
    """

    msg = str(msg_)
    logger.debug(msg)

    # convert JSON message to dict
    msg_dict = json.loads(msg)
    logger.debug(msg_dict)
    logger.debug(type(msg_dict))

    # extract tag, attrs, and content - according to SPARKL serialization
    # schema
    eventtag = str(msg_dict.get(s_tag))
    eventattrs = msg_dict.get(s_attributes, [])
    eventcontent = msg_dict.get(s_content, [])

    logger.debug(eventtag)
    logger.debug(str(eventattrs))
    logger.debug(str(eventcontent))

    # extract instanceid
    instanceid = eventattrs.get(s_instanceid)
    logger.debug(str(instanceid))

    eventid = eventattrs.get(s_id)
    logger.debug(eventid)

    subject = eventattrs.get(s_subject)
    logger.debug(subject)
    logger.debug(eventtag)

    try:
        return handle_msg_(
            instanceid, eventid, eventtag, eventattrs, eventcontent, envpath)
    except Exception as e:
        logger.error(e)
        return serialize_error_event(eventid, subject, "'"+e.message+"'")


def handle_msg_(
        instanceid, eventid, eventtag, eventattrs, eventcontent, envpath):

    if eventtag == et_open:
        handle_metadata(instanceid, eventcontent)
        return None

    elif eventtag == et_dataevent:
        subject = eventattrs.get(s_subject)
        ev_type = metadata.get(instanceid).get(s_md_optags).get(subject)
        logger.debug(ev_type)

        if ev_type == s_op_rr or ev_type == s_op_co:
            reply = handle_request(
                instanceid, eventid, eventattrs, eventcontent, envpath)
            logger.debug(str(reply))
            return reply

        elif ev_type == s_op_ow:
            reply = handle_oneway(instanceid, eventattrs, eventcontent, envpath)
            logger.debug(str(reply))
            return reply

        elif ev_type == s_op_re:
            reply = handle_response(instanceid, eventcontent)
            logger.debug(str(reply))
            return reply

    logger.debug("ignoring "+eventtag)
    return None


def handle_metadata(instanceid, eventcontent):
    """
    Handles the metadata by writing it to metadata dict

    :type: str
    :param instanceid: id of the pertaining service instance

    :type: list
    :param eventcontent: content of the received open/agg event
    """

    opreplies = {}
    optags = {}
    opids = {}
    opnames = {}
    fieldids = {}
    fieldnames = {}
    props = {}

    service = metadata.get(instanceid)

    # do we have meta data already for this service
    if service is None:
        # no meta data yet for service, so create some
        service = {
            s_md_opreplies: opreplies,
            s_md_optags: optags,
            s_md_opids: opids,
            s_md_opnames: opnames,
            s_md_fieldids: fieldids,
            s_md_fieldnames: fieldnames,
            s_md_props: props
        }
        metadata[instanceid] = service

    else:
        # yes we do have meta data already, so use it as a basis
        opreplies = service.get(s_md_opreplies)
        optags = service.get(s_md_optags)
        opids = service.get(s_md_opids)
        opnames = service.get(s_md_opnames)
        fieldids = service.get(s_md_fieldids)
        fieldnames = service.get(s_md_fieldnames)
        props = service.get(s_md_props)

    for item in eventcontent:
        # for every piece of meta data, get its tag
        itemtag = item.get(s_tag)
        logger.debug(itemtag)

        # and its attributes
        attrs = item.get(s_attributes, [])
        logger.debug(attrs)

        if itemtag == s_op_rrs or itemtag == s_op_srs \
                or itemtag == s_op_cos or itemtag == s_op_nos \
                or itemtag == s_op_res:

            ops = item.get(s_content, [])
            if ops:
                for op in ops:
                    attrs = op.get(s_attributes, [])
                    itemtag = op.get(s_tag)
                    itemid = attrs.get(s_id)
                    itemname = attrs.get(s_name)
                    logger.debug('adding op...'+itemid+":"+itemname)
                    opids[itemid] = itemname
                    if not itemtag == s_op_re:
                        opnames[itemname] = itemid
                    optags[itemid] = itemtag
                    if itemtag == s_op_co and op.get(s_content) is None:
                        optags[itemid] = s_op_ow
                    elif itemtag == s_op_co or itemtag == s_op_rr:
                        replies = op.get(s_content, [])
                        reply_md = {}
                        opreplies[itemid] = reply_md
                        for reply in replies:
                            attrs = reply.get(s_attributes, [])
                            replyid = attrs.get(s_id)
                            replyname = attrs.get(s_name)
                            reply_md[replyname] = replyid

        elif itemtag == s_fieldstag:

            # if a field, gets its id to go along with name, and add
            # to respective field dicts

            fields = item.get(s_content, [])
            for field in fields:
                attrs = field.get(s_attributes, [])
                logger.debug(attrs)

                itemtype = attrs.get(s_type)
                if itemtype:
                    itemname = attrs.get(s_name)
                    itemid = attrs.get(s_id)
                    logger.debug('adding field...'+itemid+":"+itemname)
                    fieldids[itemid] = itemname
                    fieldnames[itemname] = (itemid, itemtype)

        elif itemtag == s_service:

            # if a service, get its type and add props

            serviceattrs = item.get(s_attributes, [])
            servicetype = serviceattrs.get(s_provision)
            logger.debug(servicetype)

            if service.get(s_md_type) is None:
                service[s_md_type] = servicetype

            servicecontent = item.get(s_content, [])
            for serviceitem in servicecontent:
                serviceitemtag = serviceitem.get(s_tag)
                logger.debug(serviceitemtag)

                if serviceitemtag == s_prop:
                    serviceitemattrs = serviceitem.get(s_attributes, [])
                    propname = serviceitemattrs.get(s_name)
                    proptype = serviceitemattrs.get(s_type)
                    propvalue = serviceitem.get(s_content)[0]
                    logger.debug(propname)
                    logger.debug(propvalue)
                    props[propname] = (propvalue, proptype)

    logger.debug(str(service))


def handle_request(instanceid, eventid, eventattrs, eventcontent, envpath):
    """
    Handles incoming request operation, by calling registered callback
    (if not script service)

    :type: str
    :param instanceid: id of the pertaining service instance


    :type: str
    :param eventid: id of event

    :type: list
    :param eventattrs: attributes of the received request event

    :type: list
    :param eventcontent: content of the received request event

    :rtype: bool, dict
    :return: whether success and yielded reply

    :type: str
    :param envpath: location of instance env directory
    """

    # process the incoming field set, into a dict of field *name*/value pairs
    fields_ = process_fields_in(instanceid, eventcontent)
    logger.debug(str(fields_))

    # generate reply based on service type
    ok, outputname, outputnamedfields = \
        handle_based_on_type(instanceid, s_op_rr, eventattrs, fields_, envpath)

    logger.debug(str((ok, outputname, outputnamedfields)))

    # if all was successful, generate a reply data event
    if ok:
        fields, _ = fields_
        logger.debug(fields)
        fields.update(outputnamedfields)

        logger.debug(fields)
        opid = eventattrs.get(s_opid)
        logger.debug(opid)
        subject =  \
            metadata.get(instanceid).get(s_md_opreplies).get(opid).\
            get(outputname)
        ref = eventattrs.get(s_id)
        logger.debug(subject)

        serailized_event = \
            serialize_data_event(ref, subject, instanceid, fields)

        logger.debug(str(serailized_event))

    else:
        subject = eventattrs.get(s_subject)
        serailized_event = \
            serialize_error_event(eventid, subject, 'script result: false')

    return serailized_event


def handle_based_on_type(instanceid, eventtype, eventattrs, fields_, envpath):
    """
    Handles request or one-way based on service type.  Returns success
    and yielded outputname and fields if request/reply.

    :type: str
    :param instanceid: id of the pertaining service instance

    :type: str
    :param eventtype: type of pertaining operation event

    :type: list
    :param eventattrs: attributes of pertaining operation event

    :type: dict
    :param fields_: field set of pertaining operation, as dict of
    field *name*/value pairs

    :type: str
    :param envpath: location of instance env directory

    :rtype: bool, str, dict
    :return: success with yielded output name and fields if appropriate
    """

    logger.debug(fields_)

    outputnamedfields = {}
    outputname = ""
    ok = True

    # get the instance service type, as this will determine the course of action
    servicetype = metadata.get(instanceid).get(s_md_type)
    logger.debug(servicetype)

    # fetch the op id from the event
    opid = eventattrs.get(s_opid)
    logger.debug(opid)

    # get corresponding op name
    opname = metadata.get(instanceid).get(s_md_opids).get(opid)
    logger.debug(opname)

    fields, fieldnames = fields_
    logger.debug(fields)
    logger.debug(fieldnames)

    dockersrc = metadata. \
        get(instanceid).get(s_md_props).get(s_docker_src)

    if servicetype == s_type_containerservice and dockersrc is None:
        # if a container service, use the registered callback
        callback = callbacks.get(eventtype)
        if callback:
            logger.debug(str(callback))
            ok, outputname, outputnamedfields = callback(opname, fields)
            logger.debug(outputname)
            logger.debug(str(outputnamedfields))

    elif servicetype == s_type_scriptservice or dockersrc:
        srcprop = s_docker_src if dockersrc else s_script_src

        # if a script service, then we will execute
        # as a python or eclipse-clp script or an ansible playbook based
        # on the service instance properties which should specify a script to
        # run
        props = metadata.get(instanceid).get(s_md_props)
        scriptconfig, language = metadata.\
            get(instanceid).get(s_md_props).get(srcprop)
        props_keys = props.keys()

        op_propprefix = srcprop + '.'
        op_fileprops_keys = \
            [key for key in props_keys if key.startswith(op_propprefix)]
        op_props_ = \
            [(str(key).replace(op_propprefix, ""), props.get(key))
             for key in op_fileprops_keys]
        op_props = \
            [(key, value) for (key, (value, _type)) in op_props_]

        logger.debug(op_props)
        logger.debug(scriptconfig)
        logger.debug(fields)
        logger.debug(fieldnames)

        ok, outputname, outputnamedfields = \
            executeresults(
                instanceid,
                opname,
                language,
                scriptconfig,
                fields,
                fieldnames,
                new_collect(instanceid),
                op_props,
                envpath)
    else:
        ok = False

    logger.debug(ok, outputname, outputnamedfields)

    return ok, outputname, outputnamedfields


def handle_oneway(instanceid, eventattrs, eventcontent, envpath):
    """
    Handles incoming one way operation, by calling registered callback
    (if not script service)

    :type: str
    :param instanceid: id of the pertaining service instance

    :type: list
    :param eventattrs: attributes of the received one-way event

    :type: list
    :param eventcontent: content of the received one-way event

    :rtype: bool, dict
    :return: whether success and yielded reply (which will be None, as one-way)

    :type: str
    :param envpath: location of instance env directory
    """

    # process the incoming field set, into a dict of field *name*/value pairs
    fields = process_fields_in(instanceid, eventcontent)
    logger.debug(str(fields))

    handle_based_on_type(instanceid, s_op_ow, eventattrs, fields, envpath)
    return None


def handle_response(instanceid, eventcontent):
    """
    Handles incoming response event by calling registered callback.

    :type: str
    :param instanceid: id of the pertaining service instance

    :type: list
    :param eventcontent: content of the received response event

    :rtype: bool, dict
    :return: whether success and yielded reply (which will be None, as response)
    """

    fields, _ = process_fields_in(instanceid, eventcontent)
    callback = callbacks.get(s_op_re)
    callback(fields)
    return None


def collect_event(instanceid, opname, outputnamedfields):
    """
    Collects an event from a service instance for transport over connection.

    :type: str
    :param instanceid: id of the pertaining service instance

    :type: str
    :param opname: name of operation

    :type: dict
    :param outputnamedfields: dict of field *name*/value pairs for output field
    set

    :rtype: bool
    :return: success or not of *collecting* event for dispatch
    """

    if handle is None:
        return False

    logger.debug("generating event to send, for: "+instanceid)
    logger.debug(str(metadata.get(instanceid)))

    subject = metadata.get(instanceid).get(s_md_opnames).get(opname)
    serailized_event = \
        serialize_data_event(instanceid, subject, instanceid, outputnamedfields)

    handle(serailized_event)

    return True


def new_collect(instanceid):
    return lambda opname, outputnamedfields: \
        collect_event(instanceid, opname, outputnamedfields)


def process_fields_in(instanceid, eventcontent):
    """
    Processes incoming fields by converting from field ids/values embedded
    in a supplied eventcontent list, to a dict of field *name*/value pairs.

    :type: str
    :param instanceid: id of the pertaining service instance

    :type: list
    :param eventcontent: content of the received response event

    :rtype: dict
    :return: field *name*/value pairs
    """

    fields = {}
    fieldnames = metadata.get(instanceid).get(s_md_fieldnames)
    fieldids = metadata.get(instanceid).get(s_md_fieldids)
    for item in eventcontent:
        itemattrs = item.get(s_attributes, [])
        itemcontent = item.get(s_content)
        logger.debug(itemattrs)
        logger.debug(itemcontent)

        fieldid = itemattrs.get(s_fieldtag)

        logger.debug(fieldnames)
        logger.debug(fieldid)

        if fieldid in fieldids.keys():
            fieldvalue = itemcontent[0]
            fields[str(fieldid)] = fieldvalue

    fieldkeys = fields.keys()
    logger.debug(fieldkeys)
    if len(fieldkeys) > 0:
        if str(fieldkeys[0]).startswith('B-'):  # this should always be the case
            # we need to convert to field names
            idfields = fields
            fields = {}

            for fieldkey in fieldkeys:
                fieldname = fieldids.get(fieldkey)
                fieldvalue = idfields.get(fieldkey)
                fields[str(fieldname)] = fieldvalue

    # add the props from the service as fields
    props = metadata.get(instanceid).get(s_md_props)
    props_ = {}
    proptypes_ = {}

    for propkey in props.keys():
        propkey_ = str(propkey).replace(".", "__")
        propvalue, proptype = props[propkey]
        props_[propkey_] = str(propvalue)
        proptypes_[propkey_] = None, proptype

    fields.update(props_)
    fieldnames.update(proptypes_)

    logger.debug(str(fields))
    logger.debug(str(fieldnames))
    return fields, fieldnames


def serialize_data_event(ref, subject, instanceid, field_data):
    return {s_tag: et_dataevent,
            s_attributes: {
                s_ref: ref,
                s_subject: subject
            },
            s_content: process_fields_out(instanceid, field_data)}


def serialize_error_event(ref, subject, reason):
    return {s_tag: et_errorevent,
            s_attributes: {
                s_ref: ref,
                s_subject: subject
            },
            s_content:
                [{
                    s_tag: s_error,
                    s_attributes: {s_reason: reason}
                }]}


def process_fields_out(instanceid, fields):
    """
    Processes outgoing field-set by converting from a dict of
    field *name*/value pairs to a serialized form of field *id*/value pairs.

    :type: str
    :param instanceid: id of the pertaining service instance

    :type: dict
    :param fields: field *name*/value pairs
    """

    outfields = []
    fieldnames = metadata.get(instanceid).get(s_md_fieldnames)
    logger.debug(fields)
    logger.debug(fieldnames)

    for name in fields.keys():
        logger.debug(name + " : " + str(fieldnames.get(name)))
        field = fieldnames.get(name)
        if field:
            fieldkey, fieldtype = field
            if fieldkey and fieldtype:
                logger.debug(fields.get(name))
                outfields.append(
                    {s_tag: s_datumtag,
                     s_attributes: {
                         s_fieldtag: fieldkey
                     },
                     s_content: [fields.get(name)]})
    return outfields


def handle_internal(msg_, scriptsrc):
    msg = str(msg_)
    logger.debug(msg)

    # convert JSON message to dict
    msg_dict = json.loads(msg)
    logger.debug(msg_dict)

    fieldsin = msg_dict[s_attributes]
    logger.debug(fieldsin)
    logger.debug(scriptsrc)
    fieldsout = \
        execute_as_python(
            fieldsin[s_instanceid],
            s_main_op,
            s_undefined,
            s_undefined,
            fieldsin,
            s_undefined,
            scriptsrc)
    for key in fieldsout.keys():
        if fieldsout[key] is True:
            fieldsout[key] = "true"
        elif fieldsout[key] is False:
            fieldsout[key] = "false"
        else:
            fieldsout[key] = str(fieldsout[key])
    fieldsout[s_instanceid] = fieldsin[s_instanceid]
    logger.debug(fieldsout)
    return {s_tag: et_internal,
            s_attributes: fieldsout,
            s_content: []}
