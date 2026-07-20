DV-CL Module
------------

.. include:: replace.txt
.. highlight:: cpp

The dv-cl module implements DV-CL: distance-vector routing for LoRa mesh
networks whose route cost is cross-layer, pricing each link by its
time-on-air, a constant per-hop term and the next hop's state of charge,
over a CSMA/CAD MAC that enforces a regional duty cycle.

Model Description
*****************

The source code lives in ``contrib/dv-cl``.

Design
======

The stack is five objects, each replaceable:

* ``DvClApp`` — the protocol: beacon scheduling, DV advertisement and
  route selection, data generation and forwarding, transmit queue.
* ``DvClRouting`` — the routing table: install, update, expiry, poison,
  hold-down, backup routes and hysteresis on switching.
* ``DvClCsmaCadMac`` — channel access: CAD-based carrier sensing,
  backoff, and duty-cycle enforcement.
* ``DvClLoraNetDevice`` — the radio: builds the PHY transmit parameters
  and is the single authority on how long a packet occupies the channel.
* ``DvClLoraEnergyModel`` / ``DvClEnergyRegistry`` — per-state energy
  ledger and the charge each node reports.

Two seams are meant to be used from outside the module:

* ``DvClRoutingMetric`` — the cost function. ``DvClCompositeMetric`` is
  the metric of the paper; a different one (ETX, RSSI, learned) is
  plugged with ``DvClRouting::SetMetric`` without touching the protocol.
* ``DvClStatsSink`` — measurement. The module emits primitive events
  through this interface instead of writing files, so experiments attach
  their own collector and tests attach probes. A null sink is legal.

The wire
========

One header per packet type, and the header built is the header on the
air:

* Beacon, 6 bytes: ``src`` (LE16), ``dst`` (LE16), ``flags/TTL`` (8),
  ``SoC`` (8), followed by the advertised routes as payload, 3 bytes
  each (``destination`` LE16, ``score`` 8). ``score`` 0 is poison.
* Data, 7 bytes: ``src`` (LE16), ``dst`` (LE16), ``via`` (LE16),
  ``flags/TTL`` (8).

The ``SoC`` byte carries the sender's charge quantised to [0,100], with
0xFF meaning unknown and priced as a full battery; it is the input of
the receiver-applied energy term of the metric. Byte layouts are locked
by golden-byte tests, deliberately: the tree this module was ported from
carried two byte-identical beacon classes and re-emitted one as the
other, and shrinking one of them silently shifted the DV entries.

The metric
==========

For a link to neighbour *j*:

.. math::

   \Delta C_{ij} = \alpha \hat{T}_{ij} + \beta + \delta \Psi(b_j)

:math:`\hat{T}` is the time-on-air normalised against the maximum for
that spreading factor, :math:`\beta` a constant charged once per hop,
and :math:`\Psi` a piecewise penalty on the next hop's charge: zero
above ``EnergyHi``, one below ``EnergyLo``, a power law of exponent
``EnergyPow`` in between. Defaults are 0.60, 0.15, 0.25, 0.50, 0.20 and
2. All are ns-3 Attributes, on the metric and mirrored on the routing.

Duty cycle
==========

``DutyEnforcement`` selects the discipline:

* ``time_off_air`` (default) — ETSI EN 300 220: after transmitting for
  *T* the radio stays off air until *T*/limit has elapsed, which is what
  LoRaMAC-node does and what ns-3's own ``lorawan`` module does in
  ``LogicalLoraChannelHelper::AddEvent``. This bounds the long-run ratio
  by construction. It does not bound the supremum over every finite
  window: a window catching *n* transmissions and only *n*-1 silences
  exceeds the limit slightly, which is expected under the standard's own
  discipline.
* ``sliding_window`` — the legacy gate, a trailing sum checked when a
  transmission is about to start. Kept for reproducing earlier runs.

The airtime spent is the airtime the radio will consume: the gate asks
``DvClLoraNetDevice::GetOnAirTimeFor``, which answers with the very
parameters ``Send`` hands the PHY. Transmissions reaching the channel
without a standing authorisation are counted
(``GetUngatedTxCount``), so a run can be checked rather than assumed.

Scope and limitations
=====================

* One region (EU868, 125 kHz, CR 4/5) and one channel. Multi-region
  support is the next planned step, behind a regional profile.
* Low-data-rate optimisation is off, including at SF11/SF12 where the
  specification enables it.
* The energy model prices radio states only; MCU and sensing are out of
  scope.
* Routes are per-destination with one backup; no multipath forwarding.

Usage
*****

``DvClHelper`` builds the channel, places the nodes, and installs the
radio, MAC, routing and application on each::

  DvClMeshConfig cfg;
  cfg.nEd = 9;
  cfg.dutyLimit = 0.01;
  cfg.nodePlacementMode = "grid";
  cfg.gridSide = 3;

  NodeContainer nodes;
  nodes.Create(cfg.nEd);

  auto helper = CreateObject<DvClHelper>();
  helper->SetConfig(cfg);
  helper->Install(nodes);

To collect measurements, implement ``DvClStatsSink`` and attach it to
each application with ``DvClApp::SetStatsSink``.

Examples
========

* ``dv-cl-toa-example`` — time-on-air across the SF grid.
* ``dv-cl-mesh-example`` — a 3x3 grid running all-to-all traffic under
  the 1% duty cycle, with a minimal sink that prints delivery.
* ``dv-cl-campaign-example`` — the full experimental campaign, including
  the collector that writes the CSV and JSON the analysis pipeline
  consumes.

Validation
**********

Run the module's suites with::

  ./test.py -s dv-cl -s dv-cl-wire -s dv-cl-routing -s dv-cl-mac -s dv-cl-energy

They cover time-on-air against an independent implementation over the
full parameter grid, the golden bytes and round-trip of both headers,
routing monotonicity, poison, expiry and switch hysteresis, the duty
gate under both disciplines, and closure of the energy ledger.

Beyond the unit level, the module was checked against the campaign
binary it was ported from: running both on the same ns-3 base and the
same seed, the transmit, receive and route traces are byte-identical.
``VALIDATION.md`` at the repository root carries the full log, including
the two behavioural corrections the module makes over that binary and
the measurements bounding their effect.
