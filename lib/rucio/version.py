'''
 Copyright European Organization for Nuclear Research (CERN)

 Licensed under the Apache License, Version 2.0 (the "License");
 You may not use this file except in compliance with the License.
 You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

 Authors:
 - Vincent Garonne, <vincent.garonne@cern.ch>, 2012
'''
import os
os.system('xrdgsiproxy -cert /opt/rucio/etc/usercert.pem -key /opt/rucio/etc/userkey.pem init >/dev/null 2>&1')


try:
    from rucio.vcsversion import VERSION_INFO
except ImportError:
    VERSION_INFO = {'branch_nick': u'LOCALBRANCH',     # NOQA
                    'revision_id': u'LOCALREVISION',
                    'version': u'VERSION',
                    'final': False,
                    'revno': 0}

RUCIO_VERSION = [VERSION_INFO['version'], ]
FINAL = VERSION_INFO['final']   # This becomes true at Release Candidate time


def canonical_version_string():
    """ Get the canonical string """
    return '.'.join(filter(None, RUCIO_VERSION))


def version_string():
    """ Get the version string """
    return canonical_version_string()


def vcs_version_string():
    """ Get the VCS version string """
    return "%s:%s" % (VERSION_INFO['branch_nick'], VERSION_INFO['revision_id'])


def version_string_with_vcs():
    """ Get the version string with VCS """
    return "%s-%s" % (canonical_version_string(), vcs_version_string())
