#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import six


def text_(s, encoding='utf-8', err='strict'):
    """
    Produce an and unicode result of the specified input
    """
    if not isinstance(s, six.text_type) and s is not None:
        s = s.decode(encoding, err)
    return six.text_type(s) if s is not None else None