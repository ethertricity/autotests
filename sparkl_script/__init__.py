#!/usr/bin/python
"""
Copyright 2016 Sparkl Limited. All Rights Reserved.
Authors: Andrew Farrell <ahfarrell@sparkl.com>
Implements the engine behind the SPARKL "script" service - namely, executes
a script either as an ansible playbook, eclipse-clp script, or python function.
"""

import yaml
import json
import os
import logging
import shutil
from subprocess import call
from datetime import datetime
import imp
import sparkl_services
import random

logger = logging.getLogger(__name__)

s_value_resultfile = 'result.json'
s_value_playfile = 'playbook.yml'
s_value_eclipsefile = 'eclipse.ecl'
s_py_scriptmod = 'script'
s_py_scriptfile = s_py_scriptmod+'.py'
s_key_resultfile = '__resultfile'
s_resultvalue = 'result'
s_outputname = 'outputname'
s_fields = 'fields'
s_OK = 'Ok'
s_ERROR = 'Error'

s_script_ops = 'mixops'

cwd = os.getcwd()

LANGUAGE_ECLIPSE = "eclipse-clp"
LANGUAGE_PYTHON = "python"
LANGUAGE_ANSIBLE = "ansible"
TYPE_TIME = "time"
FIELD_TYPES = {"boolean", "float", "integer", "string"}

loaded_code = {}


def executeresults(
    instanceid,
        opname, language, script, fields, fieldnames, collect, props, envpath_):
    """
    Executes given script as an ansible playbook, once built from passed
    scriptconfig and scriptfields. Passes results back.

    :type: str
    :param instanceid: instance id of the script service

    :type: str
    :param opname: the name of the op being executed

    :type: str
    :param language: the language of script being executed

    :type: str
    :param script: script to be executed

    :type: dict
    :param fields: field map passed in original sparkl operation to
    script service, as field *name*/value pairs

    :type: dict
    :param fieldnames: fieldname meta-data map

    :type: fun
    :param collect function for notify events from service

    :type: list
    :param props: script.src props for operation

    :type: str
    :param envpath_: location of instance env directory

    :rtype: bool, str, dict
    :return: tuple giving success, output name, and output fields
    """

    # make an "environment" directory for dumping stuff
    now = str((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds()). \
        replace(".", "") + str(random.random())[1:]
    envpath = os.path.join(envpath_, now)
    envsubdir = os.path.join(envpath, sparkl_services.s_envsdir)
    resultfile = os.path.join(envpath, s_value_resultfile)

    logger.debug(envpath)
    os.makedirs(envsubdir)
    os.chdir(envsubdir)

    for file_prop_name, file_prop_value in props:
        prop_file = os.path.join(envsubdir, file_prop_name)
        logger.debug(prop_file)
        logger.debug(file_prop_value)
        dst_ = open(prop_file, 'w')
        dst_.writelines(file_prop_value)
        dst_.close()

    if language == LANGUAGE_PYTHON:
        logger.debug("executing as python...")
        ok, resultname, resultfields = \
            execute_as_python(
                instanceid,
                opname,
                envpath,
                script,
                fields,
                collect,
                sparkl_services.s_undefined)
    elif language == LANGUAGE_ECLIPSE:
        logger.debug("executing as eclipse-clp...")
        ok, resultname, resultfields = \
            execute_as_eclipse(
                opname,
                envpath,
                script,
                fields,
                fieldnames,
                resultfile)
    else:
        logger.debug("executing as ansible...")
        ok, resultname, resultfields = \
            execute_as_ansible(
                opname,
                envpath,
                script,
                fields,
                fieldnames,
                resultfile)

    shutil.rmtree(envpath)

    logger.debug(str((ok, resultname, resultfields)))

    return ok, resultname, resultfields


def getresults(resultfile, fieldnames):
    logger.debug(resultfile)

    ok = True
    resultfields = {}
    result_ = None
    resultname = s_OK

    if resultfile is not None:

        try:
            result_ = open(resultfile, 'r')
        except IOError as e:
            logger.debug(e)
            pass

        if result_ is not None:
            try:
                resultasstr = result_.read()
                logger.debug(resultasstr)
                resultmap = json.loads(resultasstr)
                result_.close()

                logger.debug(str(resultmap))

                resultvalue = resultmap.get(s_resultvalue)
                if resultvalue is False:
                    ok = False
                else:
                    resultname = resultmap[s_outputname]
                    resultfields = resultmap[s_fields]

                for fieldname in resultfields.keys():
                    (_fieldid, fieldtype) = fieldnames.get(fieldname)
                    if fieldtype == "string":
                        resultfields[fieldname] = str(resultfields[fieldname])

            except (ValueError, KeyError):
                ok = False
                resultname = s_ERROR

    return ok, resultname, resultfields


def execute_as_ansible(
        opname, envpath, scriptconfig, fields, fieldnames, resultfile):
    # parse yaml script config
    scriptconfigyaml_ = yaml.load(scriptconfig)
    logger.debug(scriptconfigyaml_)
    scriptconfigyaml = scriptconfigyaml_.get(opname.lower())

    # create playbook vars map
    pb_vars = {
        s_key_resultfile: str(resultfile)
    }

    # get any vars in the passed script config and update playbook vars map
    scriptconfigvars = scriptconfigyaml.get('vars')
    if scriptconfigvars is not None:
        pb_vars.update(scriptconfigvars)

    # update playbook vars map with the fields that are passed from initiating
    # sparkl operation
    if fields:
        pb_vars.update(fields)

    # compose playbook dict
    playbook = {
        'hosts': 'all',
        'vars': pb_vars,
        'tasks': scriptconfigyaml.get('tasks')
    }
    logger.debug(str(playbook))

    # dump playbook to file in "environment" directory
    playfile = os.path.join(envpath, s_value_playfile)
    dst_ = open(playfile, 'w')
    yaml.dump([playbook], dst_)
    dst_.close()

    # run the playbook
    playcmd = "sudo ansible-playbook -i localhost, -c local " + playfile
    logger.debug(playcmd)
    call(playcmd.split(" "), env={'PATH': '/bin:/usr/bin'})

    return getresults(resultfile, fieldnames)


def execute_as_python(
        instanceid, opname, envpath, script, fields, collect, scriptsrc):

    pymod = loaded_code.get(instanceid)
    logger.debug(pymod)
    logger.debug(scriptsrc)

    if pymod is None:
        if scriptsrc == sparkl_services.s_undefined:
            # dump script to file in "environment" directory
            scriptsrc = \
                os.path.join(envpath, s_py_scriptmod + instanceid + '.py')
            logger.debug(scriptsrc)
            dst_ = open(scriptsrc, 'w')
            dst_.writelines(script)
            dst_.close()

        # dynamically import the script
        (scriptpath, scriptname) = os.path.split(scriptsrc)
        (scriptstem, _scriptext) = os.path.splitext(scriptname)

        fp, scriptfile, description = imp.find_module(scriptstem, [scriptpath])
        pymod = imp.load_module(scriptstem, fp, scriptfile, description)
        logger.debug(pymod)
        loaded_code[instanceid] = pymod

    logger.debug(fields)
    ops = getattr(pymod, s_script_ops)
    logger.debug(ops)
    result = ops[opname](collect, fields)
    logger.debug(result)

    if result is False:
        result = False, s_ERROR, {}
    elif result is None:
        result = True, s_OK, {}

    return result


ECLIPSEDOITGOALPREFIX1 = 'do__it :- '
ECLIPSEDOITGOALPREFIX2 = ' do__it("'
ECLIPSEDOITGOALSUFFIX1 = '", '
ECLIPSEDOITGOALSUFFIX2 = ').\n'
ECLIPSEPREAMBLETHEORY = 'sparkl_eclipse_clp/preamble.ecl'


def eclipseaddfields(doitline, fields, fieldnames):

    logger.debug(str(fields))
    logger.debug(str(fieldnames))

    for fieldname in fields.keys():
        (_fieldid, fieldtype) = fieldnames.get(fieldname)
        if fieldtype not in FIELD_TYPES:
            continue
        logger.debug(fieldname)
        fieldval = fields.get(fieldname)
        logger.debug(fieldval)
        logger.debug(fieldtype)

        doitline += 'assert(ecl__field_in('+fieldname+', '
        if fieldtype == "string":
            doitline += '"'
        doitline += str(fieldval)
        if fieldtype == "string":
            doitline += '"'
        doitline += ')), '

    return doitline


def execute_as_eclipse(
        opname, envpath, script, fields, fieldnames, resultfile):

    logger.debug(__file__)

    # dump eclipse script file in "environment" directory
    eclipsefile = os.path.join(envpath, s_value_eclipsefile)
    dst_ = open(eclipsefile, 'w')

    eclipsefilelines = []

    preamblefile = os.path.join(
        __file__.split('__init__.py')[0], '..',
        ECLIPSEPREAMBLETHEORY)
    logger.debug(preamblefile)

    preamblefile_ = open(preamblefile, "r")
    preamblelines = preamblefile_.readlines()
    preamblefile_.close()

    eclipsefilelines.extend(preamblelines)

    doitline = eclipseaddfields(ECLIPSEDOITGOALPREFIX1, fields, fieldnames) +\
        ECLIPSEDOITGOALPREFIX2 + resultfile + ECLIPSEDOITGOALSUFFIX1 +\
        opname.lower() + ECLIPSEDOITGOALSUFFIX2
    logger.debug(doitline)
    eclipsefilelines.append(doitline)

    logger.debug(script)

    eclipsefilelines.extend([script])
    logger.debug(eclipsefilelines)

    dst_.writelines(eclipsefilelines)
    dst_.close()

    # run eclipse
    eclipsecmd = "eclipse -f " + eclipsefile + " -e do__it"
    logger.debug(eclipsecmd)
    call(eclipsecmd.split(" "))
    logger.debug("done executing eclipse script")

    return getresults(resultfile, fieldnames)
