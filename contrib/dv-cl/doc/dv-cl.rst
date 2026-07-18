DV-CL Module
------------

.. include:: replace.txt
.. highlight:: cpp

Model Description
*****************

The dv-cl module implements the DV-CL protocol stack: distance-vector
routing with a composite cross-layer metric (normalized time-on-air +
constant hop cost + piecewise state-of-charge penalty) and a CSMA/CAD
MAC under regional duty-cycle enforcement.

This document is a stub tracking the staged port of the protocol from
the campaign tree; the shipped units (time-on-air, composite metric) are
documented in their headers and validated by the ``dv-cl`` test suite
plus the reference scripts under ``test/reference/``.

Usage
*****

See ``examples/dv-cl-toa-example.cc`` and the module README.

Validation
**********

See ``VALIDATION.md`` at the repository root for the running validation
log (Gate 1 of the project master plan).
