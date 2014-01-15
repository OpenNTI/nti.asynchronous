#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from nti.dataserver.tests.mock_dataserver import SharedConfiguringTestBase

class ConfiguringTestBase(SharedConfiguringTestBase):
    set_up_packages = ("nti.graphdb.async",)
