# Alineación sensibilidad PHY con FLoRa (SX1272) — 2026-04-16

## Motivación

El perfil `pueyo2024_paper_like` busca replicar el paper Pueyo-Centelles 2024,
que se evalúa sobre FLoRa + OMNeT++. Verificamos en
`github.com/florasim/flora/.../LoRaReceiver.cc` que FLoRa declara:

  *Sensitivity values from Semtech SX1272/73 datasheet, table 10, Rev 3.1, March 2017*

y usa {-124, -127, -130, -133, -135, -137} dBm para SF7..SF12 a 125 kHz.

Hasta esta fecha, nuestro `SimpleGatewayLoraPhy` usaba los valores de
SX1276 ({-123, -126, -129, -132, -133, -136} dBm). La consecuencia es un
umbral de alcance SF7 de ~224 m en vez de los ~250 m que el paper usa
para justificar las separaciones de grilla de 177 m y 247 m (Fig. 8).

## Cambio aplicado

Archivo: `src/lorawan/model/simple-gateway-lora-phy.cc` (líneas 12-19)

Tabla anterior (SX1276):
```
{-123.0, -126.0, -129.0, -132.0, -133.0, -136.0}
```

Tabla actual (SX1272, FLoRa-compatible):
```
{-124.0, -127.0, -130.0, -133.0, -135.0, -137.0}
```

Los nodos mesh se instalan con `SimpleGatewayLoraPhy` (ver
`src/loramesh/helper/loramesh-helper.cc:240`). El cambio afecta a todos
los nodos del mesh. La clase `SimpleEndDeviceLoraPhy` ya usaba la tabla
SX1272 (archivo `end-device-lora-phy.cc:77`), por lo que ahora ambas
tablas coinciden.

## Verificación

- Build sólo `mesh_dv_baseline`: OK (`./ns3 build mesh_dv_baseline`).
- Smoke test N=9 grid 177 m medium, 20 pkts/pair, 2100 s:
    - TX originadas: 1317
    - RX en destino: 1014
    - PDR ≈ 77 %
    - DV tables convergidas: todas las rutas directas a 1 hop en SF7.

## Respaldo

`src/lorawan/model/simple-gateway-lora-phy.cc.bak_sx1276_20260416`

## Otros parámetros verificados contra el paper (Tablas 4 y 5)

| Parámetro                  | Paper         | Código paper_like           | Estado |
|----------------------------|---------------|-----------------------------|--------|
| Carrier freq               | 868 MHz       | 868 MHz                     | OK     |
| Bandwidth                  | 125 kHz       | 125 kHz                     | OK     |
| TX power                   | 20 dBm        | 20 dBm                      | OK     |
| Coding rate                | 4/5           | 4/5                         | OK     |
| Preamble                   | 16 sym        | 16 sym                      | OK     |
| SF min                     | SF7           | SF7                         | OK     |
| SF max (figuras 11-14)     | SF8 (restr.)  | SF8 (paper_like)            | OK     |
| Beacon period              | 60 s          | 60 s                        | OK     |
| Route expiry               | 300 s         | 60 × 5 = 300 s              | OK     |
| Max rutas/destino          | 2             | 2                           | OK     |
| Max rutas total            | 1024          | 1024                        | OK     |
| Cola routing:data          | 10:1          | 10:1                        | OK     |
| Cola forward:local         | 10:1          | 10:1                        | OK     |
| Duty cycle                 | 100 % (off)   | enableDutyCycle=false       | OK     |
| Paquetes por par           | 100           | 100                         | OK     |
| Packet size                | 27 B          | 7 hdr + 20 payload = 27 B   | OK     |
| Random order               | sí            | Fisher-Yates                | OK     |
| Path loss                  | FLoRa urban   | n=2.08, 127.41@40m          | OK     |
| Shadowing σ                | 3.57 dB       | 3.57 dB (per-evaluation)    | OK     |

La shadowing per-evaluation coincide con FLoRa: `LoRaLogNormalShadowing::
computePathLoss` llama `normal(0, sigma)` en cada evaluación; nuestro
`RandomPropagationLossModel` encadenado se comporta igual.

## Pendiente operativo

Para replicar las figuras 11-14 con low load (100 s) sin truncar la
generación, `dataStopSec` debe ser ≥ 100 × (N²−1) × 1 (suficiente para que
el nodo más lento complete su lista). Esto es config de campaña, no de
código.
