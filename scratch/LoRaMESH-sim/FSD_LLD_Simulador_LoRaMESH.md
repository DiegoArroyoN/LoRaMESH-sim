# FSD/LLD Actualizado - LoRaMESH-sim

**Proyecto:** `LoRaMESH-sim`  
**Repositorio:** `ns-3-dev/scratch/LoRaMESH-sim/`  
**Fecha de actualizacion:** 2026-03-17  
**Documento fuente de verdad:** codigo actual del workspace  

---

# 1. Proposito del documento

Este documento reemplaza la documentacion historica del simulador y describe **solo** el estado actual del codigo.  
No intenta preservar decisiones antiguas salvo cuando un modo legado sigue existiendo en el arbol fuente y puede afectar ejecucion, compatibilidad o trazabilidad.

El objetivo es dejar una referencia completa y consistente para:

- entender la arquitectura actual del simulador,
- identificar los perfiles vigentes,
- documentar el path comparable con Pueyo-Centelles,
- dejar claros los modelos PHY, MAC, routing, trafico y metricas hoy implementados,
- y servir como base para el informe y la validacion experimental.

---

# 2. Resumen ejecutivo del estado actual

## 2.1 Estado funcional actual

El simulador implementa una red LoRa mesh peer-to-peer sobre ns-3, con:

- nodos mesh identicos,
- un canal unico a 868 MHz,
- una sola recepcion activa a la vez por nodo,
- enrutamiento proactive Distance-Vector (`RoutingDv`),
- capa MAC propia con soporte para CSMA/CAD, duty-cycle y planificacion de cola,
- trafico configurable `periodic_any_to_any` y `pueyo_all_to_all`,
- exportacion de metricas detalladas a CSV y JSON,
- perfiles de ejecucion generales y perfiles comparables con Pueyo.

## 2.2 Referencia comparable principal hoy

La referencia comparable principal hoy es:

- `pueyo2024_paper_like`

Este perfil conserva la semantica comparable de `pueyo2024` y ajusta solo dos piezas que se detectaron como drift metodologico/PHY relevante respecto de la replica del paper:

- `sfMin=7`, `sfMax=8`
- `pueyoFloraLikeRx=true` (equivale a `EnableSfScanRx=false` con el mismo supuesto single-channel/single-demod)

## 2.3 Perfiles vigentes en el codigo

Los perfiles declarados hoy en `ns-3-dev/scratch/LoRaMESH-sim/mesh_dv_baseline.cc` son:

- `extended`
- `pueyo2024`
- `pueyo2024_paper_like`
- `proposal_pueyo_like`
- `proposal_pueyo_like_observed`

## 2.4 Puntos estructurales que ya no deben documentarse como antes

El estado actual del simulador **no** debe describirse como:

- un stack genericamente centrado en `wire v2` para los perfiles operativos actuales,
- un baseline comparable que use el serializer generico V2 para beacons Pueyo,
- una arquitectura gateway multi-demod,
- ni una replica paper-like basada en `SF7-12` por defecto.

Hoy, para el camino operativo actual:

- el **data wire** es `pueyo7b`,
- el **beacon path comparable** es especifico Pueyo,
- el perfil comparable principal para figuras paper-like es `pueyo2024_paper_like`,
- `extended` y `v2` quedan soportados solo como mecanismos `legacy`/compatibilidad,
- y el modelo de recepcion sigue siendo single-channel, una sola recepcion activa a la vez, una sola reception path.

---

# 3. Fuentes de verdad en el codigo

Las piezas que hoy definen el simulador son estas:

| Archivo | Rol actual |
|---|---|
| `ns-3-dev/scratch/LoRaMESH-sim/mesh_dv_baseline.cc` | entrypoint, perfiles, contratos de comparabilidad, metadata de corrida |
| `ns-3-dev/scratch/LoRaMESH-sim/mesh_dv_app.h` | interfaz principal de la aplicacion mesh |
| `ns-3-dev/scratch/LoRaMESH-sim/mesh_dv_app.cc` | control-plane, data-plane, workload, scheduler, wire paths, forwarding |
| `ns-3-dev/src/loramesh/model/loramesh-routing-dv.h` | estructuras y configuracion del routing DV |
| `ns-3-dev/src/loramesh/model/loramesh-routing-dv.cc` | logica de metricas, actualizacion DV, seleccion y mantenimiento de rutas |
| `ns-3-dev/src/loramesh/helper/loramesh-helper.h` | config generica de instalacion |
| `ns-3-dev/src/loramesh/helper/loramesh-helper.cc` | topologias, canal, instalacion de PHY, device y apps |
| `ns-3-dev/scratch/LoRaMESH-sim/mesh_lora_net_device.h` | interfaz del NetDevice mesh |
| `ns-3-dev/scratch/LoRaMESH-sim/mesh_lora_net_device.cc` | acoplamiento con PHY, TX/RX, pseudo-MAC para beacons, MTU |
| `ns-3-dev/src/lorawan/model/simple-gateway-lora-phy.h` | receptor/transmisor LoRa single-channel reutilizado como PHY mesh |
| `ns-3-dev/src/lorawan/model/simple-gateway-lora-phy.cc` | receive-start, scan/lock, immediate lock, counters PHY |
| `ns-3-dev/src/lorawan/model/lora-interference-helper.h` | interfaz del modelo de interferencia |
| `ns-3-dev/src/lorawan/model/lora-interference-helper.cc` | modelos `pueyo_fixed_capture` y `goursaud` |
| `ns-3-dev/scratch/LoRaMESH-sim/data_wire_header_v2.h` | tipos de paquete y packing `flags_ttl` |
| `ns-3-dev/scratch/LoRaMESH-sim/data_wire_header_pueyo7b.h` | header de datos estricto `pueyo7b` |
| `ns-3-dev/scratch/LoRaMESH-sim/beacon_wire_header_v2.h` | beacon V2 generico |
| `ns-3-dev/scratch/LoRaMESH-sim/beacon_wire_header_pueyo.h` | beacon comparable Pueyo |
| `ns-3-dev/scratch/LoRaMESH-sim/mesh_metric_tag.h` | tag interno de trazabilidad y forwarding |
| `ns-3-dev/scratch/LoRaMESH-sim/metrics_collector.h` | definicion de metricas y metadata |
| `ns-3-dev/scratch/LoRaMESH-sim/metrics_collector.cc` | export CSV/JSON y resumen por corrida |

---

# 4. Arquitectura actual del simulador

## 4.1 Vista de alto nivel

El flujo actual es:

1. `mesh_dv_baseline.cc` parsea CLI y selecciona perfil.
2. Se derivan parametros comparables o generales.
3. `LoraMeshHelper` instala nodos, topologia, canal, `MeshLoraNetDevice`, `SimpleGatewayLoraPhy` y `MeshDvApp`.
4. `MeshDvApp` inicializa routing, cola TX, generador de datos y beacons DV.
5. `RoutingDv` mantiene rutas primarias, backup e inbound.
6. `MeshLoraNetDevice` entrega los paquetes al PHY y expone callbacks RX a la app.
7. `SimpleGatewayLoraPhy` modela transmision, recepcion y destruccion por interferencia.
8. `MetricsCollector` consolida eventos, backlog, PDR, delay, control-plane y metadata de corrida.

## 4.2 Componentes y responsabilidad real

### `MeshDvApp`

Responsabilidades actuales:

- generacion de trafico de datos,
- envio y recepcion de beacons DV,
- forwarding multi-hop,
- seleccion local de SF de datos hacia el siguiente salto,
- resolucion de direccion de enlace para unicast,
- integracion con `RoutingDv`,
- integracion con la cola MAC, duty-cycle y CSMA/CAD,
- snapshots de control-plane, traces y contadores operativos.

### `RoutingDv`

Responsabilidades actuales:

- mantener rutas primarias `m_routes`,
- mantener rutas backup `m_backupRoutes`,
- mantener rutas inbound `m_inboundRoutes`,
- procesar mensajes DV recibidos,
- construir anuncios de rutas para beacons,
- soportar modo `toa_only` y `composite_score`,
- soportar `cost255`, `score255` y `score100`,
- aplicar expiracion, poison, backup promotion y limites de tabla.

### `MeshLoraNetDevice`

Responsabilidades actuales:

- encapsulacion final TX hacia `LoraPhy`,
- recepcion desde el PHY hacia la app,
- configuracion de `TxPowerDbm`, `PreambleSymbols` y `WireFormat`,
- aprendizaje de pseudo-direccion de origen desde beacons no-v1,
- integracion con el modelo de energia del dispositivo,
- volcado PCAP por nodo y PCAP global.

### `SimpleGatewayLoraPhy`

Responsabilidades actuales:

- transmitir por el canal LoRa,
- gestionar una sola recepcion activa a la vez,
- opcionalmente hacer `RX-scan/lock` previo a la recepcion,
- exponer contadores de scan, locks, misses e interferencia,
- decidir entrega final via `LoraInterferenceHelper`.

### `LoraInterferenceHelper`

Responsabilidades actuales:

- registrar eventos superpuestos en el canal,
- evaluar destruccion por interferencia,
- soportar el modelo comparable `pueyo_fixed_capture`,
- soportar el modelo alternativo `goursaud`.

### `MetricsCollector`

Responsabilidades actuales:

- registrar eventos TX/RX/routing/delay/energy/overhead,
- exportar CSVs crudos por categoria,
- exportar un resumen JSON muy rico por corrida,
- adjuntar metadata completa del setup,
- consolidar contadores de control-plane, PHY y backlog.

---

# 5. Perfiles de ejecucion vigentes

## 5.1 Idea general

El simulador tiene un entrypoint unico (`mesh_dv_baseline.cc`) y varios perfiles.  
Un perfil no define por si solo una campana completa: define el contrato de protocolo/PHY/MAC/metrica. Los runners externos ajustan `nEd`, topologia, tiempos, seeds y sweep de parametros.

Antes de aplicar overrides por linea de comando, el entrypoint arranca hoy con defaults operativos paper-like:

- `profile = pueyo2024_paper_like`
- `wireFormat = pueyo7b`
- `trafficMode = periodic_any_to_any`
- `trafficLoad = medium`
- `nEd = 10`
- `stopSec = 150`
- `dataStartSec = 90`
- `enableCsma = true`
- `enableDutyCycle = true` con `dutyLimit = 0.01`
- `routeMetricMode = composite_score`
- `sfLinkMode = observed_rxsf`
- `sfMin = 7`, `sfMax = 12`
- `txPowerDbm = 14`
- `preambleSymbols = 8`
- `interferenceModel = puello`
- `enableSfScanRx = true`
- `nodePlacementMode = random` sobre `1000 x 1000 m`

Estos valores son la base de arranque del binario. En la practica, los perfiles comparables y los runners de campana siguen sobreescribiendo gran parte de la configuracion.

## 5.2 Tabla de perfiles

| Perfil | Proposito | Wire de datos | Beacon path | Metrica DV | `sfLinkMode` | CSMA | Duty | Rango SF | Receive-start | Interferencia |
|---|---|---|---|---|---|---:|---:|---|---|---|
| `extended` | perfil legacy de ingenieria / experimentacion | `v2` | `v2` | `composite_score` | `observed_rxsf` | si | si | `7-12` | scan/lock por defecto | `puello` |
| `pueyo2024` | baseline comparable estricto | `pueyo7b` | Pueyo | `toa_only` | `deterministic_sensitivity` | no | no | `7-12` | scan/lock por defecto | `pueyo_fixed_capture` |
| `pueyo2024_paper_like` | mejor aproximacion actual al paper | `pueyo7b` | Pueyo | `toa_only` | `deterministic_sensitivity` | no | no | `7-8` | FLoRa-like immediate lock | `pueyo_fixed_capture` |
| `proposal_pueyo_like` | propuesta comparable contra Pueyo | `pueyo7b` | Pueyo | `composite_score` | `deterministic_sensitivity` | si | si (1%) | `7-12` | scan/lock por defecto | `pueyo_fixed_capture` |
| `proposal_pueyo_like_observed` | propuesta comparable sin oraculo de SF | `pueyo7b` | Pueyo | `composite_score` | `observed_rxsf` | si | si (1%) | `7-12` | scan/lock por defecto | `pueyo_fixed_capture` |

## 5.3 Base comparable Pueyo

La base comparable aplicada por `mesh_dv_baseline.cc` fija hoy, para los perfiles Pueyo-like:

- `wireFormat = pueyo7b`
- `txPowerDbm = 20`
- `preambleSymbols = 16`
- `interferenceModel = pueyo_fixed_capture`
- `sfMin = 7`, `sfMax = 12`
- `trafficMode = pueyo_all_to_all`
- `pueyoPacketsPerPair = 100`
- `enableDataRandomDest = false`
- `beaconIntervalWarmSec = 60`
- `beaconIntervalStableSec = 60`
- `routeTimeoutFactor = 5`
- `routeAdvertPolicy = cost_weighted`
- `costEncoding = cost255`
- `maxRoutesPerDestination = 2`
- `maxTotalRoutes = 1024`
- `dvPayloadMaxBytes = 251`
- `dataPayloadSizeBytes = 20`
- `shadowingSigmaDb = 3.57`

## 5.4 Distincion clave entre `pueyo2024` y `pueyo2024_paper_like`

`pueyo2024` es el baseline comparable estricto actual del codigo.  
`pueyo2024_paper_like` es un clon controlado que conserva esa base y cambia solo:

- `sfMin = 7`
- `sfMax = 8`
- `pueyoFloraLikeRx = true`

Todo lo demas permanece alineado con la base comparable.

## 5.5 `extended` no es el baseline del paper

`extended` sigue existiendo en el codigo, pero no debe usarse como referencia paper-like.  
Es el perfil de desarrollo y experimentacion general.

---

# 6. Topologia, canal y supuestos fisicos

## 6.1 Nodos mesh

El simulador crea `nEd` nodos mesh identicos.  
No existe una clase distinta para gateway LoRaWAN ni multi-demod; todos los nodos son peers mesh.

Aun asi, el ultimo nodo (`nEd-1`) se usa como `collectorNodeId`/sink de referencia para:

- observabilidad,
- metricas de conectividad,
- y algunos caminos genericos de priorizacion en el modo no-Pueyo.

Esto **no** convierte la red en star topology: el trafico puede seguir siendo any-to-any.

En la instalacion de movilidad actual:

- todos los nodos salvo el ultimo se ubican con `z = 0`
- el ultimo nodo usa `z = gwHeight`

El nombre `gwHeight` es herencia historica del runner, pero hoy no implica una clase de gateway separada.

## 6.2 Topologias soportadas

`LoraMeshHelper` soporta actualmente:

- `line`
- `random`
- `pueyo_grid`
- `pueyo_random_equiv`

### `pueyo_grid`

Requiere que `nEd` sea un cuadrado perfecto.  
Ubica nodos en una grilla `side x side` con spacing configurable.

### `pueyo_random_equiv`

Usa la misma cantidad de nodos que la grilla y genera posiciones uniformes en un area equivalente a:

- ancho = `(side - 1) * spacingX`
- alto = `(side - 1) * spacingY`

Esto es el preset usado para comparacion Fig. 11-12 tipo Pueyo.

## 6.3 Canal y propagacion

El helper instala hoy:

- `LogDistancePropagationLossModel`
- `ConstantSpeedPropagationDelayModel`
- sombra opcional con `RandomPropagationLossModel` y variable normal

El entrypoint `mesh_dv_baseline.cc` sobreescribe los defaults del helper y usa como preset principal:

- exponente de perdida `n = 2.08`
- distancia de referencia `d0 = 40 m`
- perdida en referencia `L0 = 127.41 dB`
- `shadowingSigmaDb = 3.57`

## 6.4 Banda y canalizacion

El simulador actual usa:

- frecuencia central unica: `868000000 Hz`
- `channelCount = 1`
- ancho de banda LoRa: `125000 Hz`
- coding rate por defecto de TX: `4/5`

Supuesto actual: **single-channel**.

---

# 7. PHY actual y modelo de recepcion

## 7.1 Clase PHY utilizada

El PHY instalado en cada nodo es `SimpleGatewayLoraPhy`, pero se usa como componente reutilizado para mesh, **no** como gateway LoRaWAN multi-demod.

## 7.2 Restriccion real de recepcion

Cada nodo se instala con:

- una sola frecuencia (`AddFrequency(868000000)`)
- una sola `reception path`

En `LoraMeshHelper::InstallDevices()` el bucle actual es explicitamente:

- `for (int p = 0; p < 1; ++p) phy->AddReceptionPath();`

Con esto, el supuesto actual es:

- single-channel,
- single-demod,
- una sola recepcion activa a la vez.

## 7.3 Sensibilidad usada

`SimpleGatewayLoraPhy` redefine la tabla de sensibilidad como valores tipo SX1276 a 125 kHz:

| SF | Sensibilidad (dBm) |
|---|---:|
| 7 | -123 |
| 8 | -126 |
| 9 | -129 |
| 10 | -132 |
| 11 | -133 |
| 12 | -136 |

## 7.4 Dos modos de receive-start

### Modo normal (`EnableSfScanRx=true`)

Es el receive model actual de ns-3 mesh con estados:

- `IDLE`
- `SCAN`
- `RX_LOCK`

Comportamiento:

1. una senal entrante se agrega a `m_pendingSignals`,
2. el PHY hace barrido CAD SF por SF,
3. si detecta una senal viable para un SF escaneado, hace lock,
4. solo entonces agenda `EndReceive()`.

Contadores relevantes:

- `rxScanAttempts`
- `rxScanLocks`
- `rxScanMissBeforeLock`
- `rxPostLockInterferenceFail`
- `rxNoMoreDemodDrops`
- `rxScanTimeTotalS`

### Modo FLoRa-like simplificado (`EnableSfScanRx=false`)

Cuando el perfil activa `pueyoFloraLikeRx=true`, el entrypoint fuerza:

- `EnableSfScanRx = false`

Con eso el PHY conserva:

- single-channel,
- una sola recepcion activa a la vez,
- el mismo modelo de interferencia,

pero elimina el paso de `pending + scan + lock` y hace lock inmediato en el SF verdadero del evento, si hay un path libre y la potencia supera sensibilidad.

## 7.5 `pueyoFloraLikeRx`

`pueyoFloraLikeRx` no crea una clase PHY nueva.  
Es un flag de perfil/harness que hoy significa:

- misma clase `SimpleGatewayLoraPhy`
- mismo supuesto single-channel / single-demod
- mismo collision/capture model
- **solo** desactivar `EnableSfScanRx`

Esto fue introducido para aproximar el receive-start esperado en FLoRa sin convertir nodos mesh en gateways.

## 7.6 Que no hace el simulador actual

El simulador actual **no** hace esto:

- multiples reception paths por nodo,
- multi-demod concurrente tipo SX1301/SX1302,
- multicanal LoRaWAN gateway.

---

# 8. Modelo de colision e interferencia

## 8.1 Modelos disponibles

`LoraInterferenceHelper` soporta dos modelos:

- `pueyo_fixed_capture`
- `goursaud`

## 8.2 Modelo comparable actual: `pueyo_fixed_capture`

Es el modelo usado por:

- `pueyo2024`
- `pueyo2024_paper_like`
- `proposal_pueyo_like`
- `proposal_pueyo_like_observed`

Reglas actuales:

- solo se consideran interferentes en la misma frecuencia,
- y solo interferentes del mismo SF,
- cross-SF se ignora en este modelo,
- la destruccion depende de:
  - margen de potencia respecto a `PuelloCaptureThresholdDb`,
  - y de una ventana critica de preambulo basada en `PuelloPreambleSymbols`.

## 8.3 Modelo alternativo: `goursaud`

`goursaud` acumula energia interferente por SF y usa una matriz SNIR.  
Puede modelar interferencia cross-SF y captura probabilistica si esa opcion esta habilitada.

## 8.4 Posicion actual en el simulador

La referencia principal `pueyo2024_paper_like` mantiene `pueyo_fixed_capture`.  
Las campanas de sensibilidad muestran que pasar a `goursaud` es util para amenazas a la validez, pero no esta integrado como default del perfil principal.

---

# 9. Capa MAC, cola y planificacion de transmision

## 9.1 Componente MAC

La cola y el acceso al medio se resuelven desde `MeshDvApp` usando `loramesh::CsmaCadMac`.

## 9.2 Elementos actuales de la politica de TX

La cola actual soporta:

- entradas de beacon y de data en una misma cola,
- razon de espera (`pendingReason`) y contadores de defer,
- reordenamiento de la cola segun scheduler activo,
- chequeo de control guard,
- CAD/backoff si CSMA esta habilitado,
- chequeo de duty-cycle antes de transmitir,
- reconstruccion del beacon y asignacion de `rp_counter` justo antes del TX real.

## 9.3 Dos politicas de scheduler

### Scheduler generico

Si no aplica el modo estricto Pueyo, `ProcessTxQueue()` usa un selector legacy anti-starvation entre beacon y data.

### Scheduler Pueyo estricto

Si se cumplen ambas condiciones:

- `enableCsma = false`
- `pueyoStrictQueueScheduler = true`

entonces `ProcessTxQueue()` usa `SelectStrictQueueHead()` antes de transmitir.

## 9.4 CSMA/CAD y duty-cycle

Cuando `enableCsma=true`:

- se ejecuta `PerformChannelAssessment()` sobre el contexto del SF del paquete,
- si el canal esta ocupado se agenda backoff,
- el backoff puede escalarse con:
  - `controlBackoffFactor`
  - `dataBackoffFactor`

Cuando el duty-cycle bloquea TX:

- el paquete queda en cola,
- se incrementan contadores separados para control y data,
- se reintenta mas tarde.

## 9.5 Control guard y latest-only

El stack actual conserva varias optimizaciones opcionales:

- `EnableControlGuard`
- `PrioritizeBeacons`
- `BeaconLatestOnly`
- `ExtraDvBeacon*`

Estas pueden ser utiles en perfiles generales o de propuesta, pero no forman parte del baseline comparable paper-like puro.

---

# 10. Wire protocols y formato on-air

## 10.1 Distincion clave: tag interno vs wire on-air

El simulador usa `MeshMetricTag` como metadata interna de forwarding y trazabilidad.  
Ese tag **no** debe confundirse con el wire on-air.

El wire on-air actual depende del `wireFormat` y del tipo de paquete.

## 10.2 Tipos logicos de paquete

`data_wire_header_v2.h` define hoy:

- `WirePacketTypeV2::DATA = 0`
- `WirePacketTypeV2::BEACON = 1`

El byte `flags_ttl` se empaqueta como:

- bits `[7:6]`: tipo
- bits `[5:0]`: TTL o `rp_counter`

`PackFlagsTtlV2()` trunca el valor de TTL/counter a `0..63`.

## 10.3 Formatos de datos soportados

### `v1` (legado)

- usa `MeshMacHeader` explicito L2,
- no es el wire comparable principal,
- se conserva por compatibilidad.

En `v1`, el payload DV legado usa `MeshMetricTag::RoutePayloadEntry` con tamano de:

- `destination` (2 B)
- `hops` (1 B)
- `sf` (1 B)
- `score` (1 B)
- `battery` (1 B)

Tamano por entrada legado: **6 bytes**.

### `v2` legacy

Header `DataWireHeaderV2`:

- `src` (2 B)
- `dst` (2 B)
- `via` (2 B)
- `flags_ttl` (1 B)
- `seq16` (2 B)

Tamano total: **9 bytes**.

### `pueyo7b`

Header `DataWireHeaderPueyo7b`:

- `src` (2 B)
- `dst` (2 B)
- `via` (2 B)
- `flags_ttl` (1 B)

Tamano total: **7 bytes**.

Observacion importante actual:

- en `pueyo7b`, la secuencia de datos no viaja on-air en el header de datos;
- la secuencia completa para deduplicacion y metricas sigue en `MeshMetricTag`.

## 10.4 Formatos de beacon soportados

### `v2` generico legacy

`BeaconWireHeaderV2`:

- `src` (2 B)
- `dst` (2 B)
- `flags_ttl` (1 B)

Tamano total: **5 bytes**.

Cada entrada DV V2 ocupa:

- `destination` (2 B)
- `score` (1 B)

Tamano por entrada: **3 bytes**.

### Beacon comparable Pueyo

`BeaconWireHeaderPueyo` usa la misma estructura fisica de 5 bytes:

- `src` (2 B)
- `dst` (2 B)
- `flags_ttl` (1 B)

Cada entrada Pueyo actual ocupa:

- `destination` (2 B)
- `score` (1 B)

Tamano por entrada: **3 bytes**.

## 10.5 Path de beacon real en perfiles comparables

Hoy, si `wireFormat == pueyo7b`:

- los **datos** usan `DataWireHeaderPueyo7b`,
- los **beacons** usan `BuildAndSendDvPueyo()`, `ParseBeaconWirePacketPueyo()` y `DecodeDvEntriesPueyo()`.

Esto ya no depende semanticamente del serializer generico V2 para el baseline comparable.

## 10.6 `rp_counter` en beacons

En el wire no-v1, el lower 6-bit field del beacon se usa como `rp_counter`:

- se asigna justo antes del TX real en `ProcessTxQueue()`,
- avanza modulo 64,
- en recepcion se extiende a una secuencia monotona local por origen con `ResolveBeaconSequenceFromRpCounter()`.

## 10.7 Presupuesto real de beacon comparable

La capacidad de beacon se calcula con `GetBeaconRouteCapacity()`.

Para perfiles comparables con:

- `dvPayloadMaxBytes = 251`
- `entrySize = 3`

la capacidad efectiva es:

- `floor(251 / 3) = 83` entradas

Consecuencias:

- payload DV real maximo = `83 * 3 = 249 bytes`
- header beacon = `5 bytes`
- paquete beacon total = `254 bytes`

Es decir:

- el budget de payload anunciado es 251 B,
- pero el payload real ocupado por entradas cabe en multiplos de 3,
- por eso el limite efectivo actual es 249 B de entries.

## 10.8 MTU real en la ruta mesh

`MeshLoraNetDevice::GetMtu()` contiene una tabla SF->MTU pensada para `EndDeviceLoraPhy`, pero el simulador mesh usa `SimpleGatewayLoraPhy`.  
Por lo tanto, en la ruta real actual:

- el MTU efectivo se mantiene en el configurado por `SetMtu(255)`
- y no cambia dinamicamente con SF en el device mesh.

---

# 11. Routing DV actual

## 11.1 Estructuras de ruta vigentes

`RouteEntry` contiene hoy:

- `destination`
- `nextHop`
- `seqNum`
- `hops`
- `sf`
- `toaUs`
- `batt_mV`
- `scoreX100`
- `costX1000`
- `toaCostUnits`
- `rawMetric`
- `lastUpdate`
- `expiryTime`
- `nextHopMac`

La tabla actual de rutas se separa en:

- `m_routes` (primarias)
- `m_backupRoutes` (backup)
- `m_inboundRoutes` (reachability inbound explicita)

## 11.2 Metricas soportadas

### `toa_only`

`RoutingDv::ToaHopCostUnits(sf)` define el costo por salto como:

| SF | unidades ToA |
|---|---:|
| 7 | 1 |
| 8 | 2 |
| 9 | 4 |
| 10 | 8 |
| 11 | 16 |
| 12 | 32 |

En `toa_only`:

- el beacon anuncia un costo de camino cuantizado segun `CostEncoding`,
- el receptor decodifica ese costo anunciado,
- suma el costo local del enlace al siguiente salto,
- y vuelve a calcular el `score` local comparable.

### `composite_score`

La logica authoritative del costo compuesto hoy esta en `RoutingDv`, no en el helper grafico del paper.  
El candidato se construye sumando:

- incremento local de ToA ponderado (`CompositeWToa * ToaHopCostUnits(sf)`)
- incremento por hop (`CompositeWHop`)
- penalizacion energetica local (`ComputeCompositeEnergyPenalty()`)

El helper `CompositeMetric` sigue existiendo y se usa para ciertos calculos de enlace/compatibilidad local, pero la comparacion final de rutas y la acumulacion de costo end-to-end se resuelven en `RoutingDv`.

## 11.3 Codificacion on-air del costo

El routing soporta:

- `score100`
- `cost255`
- `score255`

La base comparable Pueyo usa hoy:

- `cost255`

Semantica actual:

- `0` = unreachable/poison
- `1..255` = costo/anuncio valido

## 11.4 Actualizacion al recibir un beacon

`UpdateFromDvMsg()` hace hoy este flujo:

1. valida origen, vecino, secuencia y cantidad de entradas,
2. construye una ruta directa al vecino emisor,
3. la guarda explicitamente tambien en `m_inboundRoutes`,
4. procesa cada `destination + score` anunciados,
5. para cada entrada arma un `candidate` usando el enlace local al originador,
6. pasa el candidato a `UpdateRoute()`.

## 11.5 Inbound routes

Las inbound routes ya no quedan solo como reconstruccion accidental.  
El estado actual mantiene `m_inboundRoutes` explicitamente y el beacon path Pueyo las considera en la seleccion de rutas anunciables.

## 11.6 Seleccion de rutas para beacon

### `GetBestRoutes()`

Es la politica generica del simulador.  
Puede incluir:

- priorizacion de destinos activos,
- priorizacion del sink,
- poison prioritario,
- `TOP_SCORE`, `UNIFORM` o `COST_WEIGHTED` segun perfil.

### `GetBestRoutesPueyo()`

Es la politica baseline-only del beacon comparable.  
Hoy hace esto:

- arma un candidate set a partir de `m_routes` y `m_inboundRoutes`,
- no usa prefases de destinos activos ni sink prioritario,
- si todo cabe, ordena por mejor costo,
- si no cabe, selecciona sin reemplazo con muestreo `cost_weighted` puro.

## 11.7 Regla de comparacion entre rutas

`IsCandidateBetter()` compara hoy por:

1. usabilidad (score 0 vs score > 0)
2. costo comparable (`GetComparableMetric()`)
3. SF mas bajo
4. desempate aleatorio si persiste empate y cambia next-hop

## 11.8 Expiracion, poison y limpieza

El routing actual soporta:

- timeout de ruta,
- poison announcements,
- purge,
- backup promotions,
- hold-down de destinos recientemente fallidos,
- y limites globales de tabla con eviction.

---

# 12. Beacon path comparable Pueyo

## 12.1 Objetivo del path Pueyo

El beacon path comparable se creo para desacoplar el baseline Pueyo del serializer generico V2 y dejar una semantica de control-plane especifica y auditable.

## 12.2 Flujo de TX de beacon comparable

Cuando `m_wireFormat == pueyo7b`, `BuildAndSendDv()` despacha a `BuildAndSendDvPueyo()`.

Ese metodo hoy:

1. calcula `maxRoutes` con `GetBeaconRouteCapacity()`,
2. si `m_purePueyoBaselineMode=true`, pide rutas via `GetBestRoutesPueyo(maxRoutes)`,
3. serializa entries `destination + score` con rango `0..255`,
4. construye `BeaconWireHeaderPueyo`,
5. adjunta `MeshMetricTag` para trazas/ToA,
6. agenda el envio via `SendWithCSMA()`.

## 12.3 Flujo de RX de beacon comparable

`L2ReceiveV2()` hace hoy:

1. extrae `rxSf` y `rxPowerDbm` desde `LoraTag`,
2. parsea beacon con `ParseBeaconWirePacketPueyo()`,
3. aprende `nodeId -> pseudo-Mac48` desde el `src` logico del beacon,
4. actualiza historial empirico de SF del vecino,
5. resuelve `linkSf` segun el modo de enlace,
6. extiende la secuencia via `rp_counter`,
7. decodifica entries con `DecodeDvEntriesPueyo()`,
8. llama `RoutingDv::UpdateFromDvMsg()`.

## 12.4 Pseudo-MAC de beacons no-v1

Como los beacons Pueyo/V2 no cargan `MeshMacHeader`, `MeshLoraNetDevice::Receive()` sintetiza una direccion `Mac48` estable a partir del `src` logico:

- formato: `02:00:00:00:hi:lo`

Eso se usa solo como wrapper interno de ns-3 para poder entregar un `from` direccionable al nivel superior y poblar la tabla `nodeId -> linkAddr`.

La identidad on-air sigue siendo el `src` logico de 2 bytes.

---

# 13. Data-plane actual

## 13.1 Generacion de datos

`MeshDvApp` puede operar en:

- `periodic_any_to_any`
- `pueyo_all_to_all`

En ambos casos, la app genera datos desde `dataStartTimeSec`, respeta `dataStopTimeSec` si existe y usa `DataPayloadSizeBytes` como payload de aplicacion.

## 13.2 `pueyo_all_to_all`

`BuildPueyoTrafficSchedule()` construye una agenda por nodo donde:

- cada destino distinto de si mismo aparece `PueyoPacketsPerPair` veces,
- la agenda se randomiza por nodo con Fisher-Yates,
- el workload total por nodo es `PueyoPacketsPerPair * (N - 1)`.

Con la base comparable vigente:

- `PueyoPacketsPerPair = 100`

por lo tanto:

- paquetes por nodo = `100 * (N - 1)`
- paquetes totales de la red = `N * 100 * (N - 1)`

## 13.3 Cargas disponibles

`UpdateDataPeriod()` fija hoy:

| Load | Periodo |
|---|---:|
| `low` | 100 s |
| `medium` | 10 s |
| `high` | 1 s |
| `saturation` | 0.1 s |

## 13.4 Seleccion de SF para datos

El SF de datos hacia el siguiente salto puede resolverse de dos maneras:

### `deterministic_sensitivity`

- usa `ResolveSfForLink()`
- deriva el SF minimo viable desde `rxPowerDbm` y la tabla de sensibilidad SX1276
- luego lo clampa al rango `[sfMin, sfMax]`

### `observed_rxsf`

- usa el historial empirico de beacons recibidos por vecino
- soporta `EmpiricalSfSelectMode=robust_min`
- exige `EmpiricalSfMinSamples`
- si no hay evidencia suficiente, cae a un fallback conservador

## 13.5 Resolucion del siguiente salto

Al transmitir datos, la app:

1. consulta `RoutingDv::GetRoute(dst)`,
2. obtiene `nextHop`,
3. resuelve la direccion de enlace desde la cache aprendida en RX,
4. si no existe `linkAddr` para unicast, el paquete se descarta como `no_route`/`no_link_addr_for_unicast`.

## 13.6 Anti-backtrack y deduplicacion

El data-plane actual incluye:

- `AvoidImmediateBacktrack`
- deduplicacion por ventana `DedupWindowSec`
- cache `m_seenPackets`

Esto evita loops triviales y duplicados durante forwarding multi-hop.

---

# 14. Modelo temporal del experimento

## 14.1 Tiempos relevantes

La corrida expone hoy estos tiempos:

- `dataStartSec`
- `dataStopSec`
- `stopSec`
- `pdrEndWindowSec`

`mesh_dv_baseline.cc` los exporta a `RunConfigMetadata` y `MetricsCollector` los vuelca al JSON final.

## 14.2 Warm-up y fase de datos

El simulador separa conceptualmente:

- una fase de warm-up de control-plane,
- una fase de generacion de datos,
- un posible tramo de drenaje hasta `stopSec`.

La madurez de la topologia no se asume: se mide con metricas como:

- `firstUsableRouteTimeSec`
- `coverage80RouteTimeSec`
- `routesAtDataStart`
- `routesAtMidpoint`
- `routesAtDataStop`

## 14.3 Importante: el perfil no fija por si solo el horizonte completo

Ni `pueyo2024` ni `pueyo2024_paper_like` calculan automaticamente el horizonte temporal del paper.  
Eso lo hacen los runners de campana.

Esto es especialmente importante en `low`, donde completar el workload completo de `100 packets per pair` requiere ventanas de datos muy largas.

## 14.4 Formula para full workload Pueyo

Para `pueyo_all_to_all`:

- paquetes por nodo = `100 * (N - 1)`
- duracion de datos = `packets_per_node * interval`

Ejemplo `low`:

- intervalo = `100 s`
- `N=9` -> `800` paquetes por nodo -> `80000 s` de fase de datos
- `N=25` -> `2400` paquetes por nodo -> `240000 s` de fase de datos

La version principal del perfil no cambia por esto; el horizonte se ajusta desde el harness cuando se requiere paridad temporal completa.

---

# 15. Energia

## 15.1 Modelo actual de energia

El simulador usa hoy dos capas relacionadas:

### `loramesh::EnergyModel`

Se usa dentro de la logica mesh y routing para:

- obtener energia remanente,
- obtener fraccion de energia,
- alimentar componentes de metricas y costo compuesto.

### `LoRaDeviceEnergyModel` del Energy Framework

`mesh_dv_baseline.cc` instala:

- `BasicEnergySource` por nodo
- `LoRaDeviceEnergyModel` por device mesh

Configuracion actual del source:

- capacidad total `BatteryFullCapacityJ` (default 38880 J)
- SOC inicial uniforme en `[0.60, 1.00]`
- voltaje nominal `3.6 V`

Corrientes base configuradas:

- TX auto-ajustada por `txPowerDbm`
- RX `0.011 A`
- CAD `0.011 A`
- IDLE `0.001 A`
- SLEEP `0.0000002 A`

## 15.2 Uso funcional actual

La energia actual influye en:

- metricas y reportes,
- costo compuesto,
- consumo del dispositivo durante TX/RX/CAD/IDLE.

En el baseline comparable `toa_only`, la energia sigue registrandose, pero no gobierna la metrica principal.

---

# 16. Metricas, export y artefactos

## 16.1 Export core por corrida

`mesh_dv_baseline.cc` exporta al final:

- CSVs con prefijo `mesh_dv_metrics_*`
- JSON resumen con prefijo `mesh_dv_summary`

Los CSV crudos actualmente son:

- `mesh_dv_metrics_tx.csv`
- `mesh_dv_metrics_rx.csv`
- `mesh_dv_metrics_routes.csv`
- `mesh_dv_metrics_routes_used.csv`
- `mesh_dv_metrics_delay.csv`
- `mesh_dv_metrics_overhead.csv`
- `mesh_dv_metrics_duty.csv`
- `mesh_dv_metrics_energy.csv`
- `mesh_dv_metrics_lifetime.csv`

## 16.2 Metadata por corrida

`RunConfigMetadata` hoy incluye, entre otras cosas:

- version y commit,
- perfil,
- topologia y geometria,
- carga y modo de trafico,
- tiempos de inicio/parada,
- payload, packets-per-pair,
- `sfMin/sfMax`, `preambleSymbols`, `txPowerDbm`,
- `routeAdvertPolicy`, `routeMetricMode`, `costEncoding`, `sfLinkMode`,
- limites de tabla y beacon,
- `pueyoFloraLikeRx`, `EnableSfScanRx`, `sfScanEdThresholdDbm`,
- `shadowingSigmaDb`, `interferenceModel`,
- `channelCount`, `receptionPaths`,
- `wireFormat`, `dataHeaderBytes`, `beaconHeaderBytes`, `dvEntryBytes`.

## 16.3 Resumen JSON

El JSON consolidado actual contiene secciones como:

- `simulation`
- `pdr`
- `pdr_by_source`
- `delivery_by_destination`
- `tx_attempts`
- `forwarding`
- `throughput`
- `delay`
- `energy`
- `overhead`
- `routes`
- `control_plane`
- `quantization`
- `queue_backlog`
- `drops`
- `thesis_metrics`

## 16.4 Definiciones importantes actuales

El resumen exporta explicitamente:

- `delivery_ratio`
- `pdr_post_convergence`
- `pdr_no_drain`
- `source_first_tx_count`
- `delivered_per_tx_attempt`
- `forwarded_unique_count`
- `routes_total`
- `control_tx_sent`
- `data_tx_sent`
- `rx_scan_miss_before_lock`
- `rx_post_lock_interference_fail`
- `queued_packets_end`

Esto permite separar:

- generacion,
- admision a TX,
- forwarding efectivo,
- entrega final,
- perdidas de control-plane,
- backlog,
- y perdidas PHY.

---

# 17. Runners y capa de campanas

## 17.1 Entry point base

La simulacion base sigue entrando por:

- `ns-3-dev/scratch/LoRaMESH-sim/mesh_dv_baseline.cc`

## 17.2 Runners de campana presentes en el arbol

El directorio `scratch/LoRaMESH-sim/` contiene runners Python para campañas y auditorias.  
Entre los que hoy forman parte del estado real del proyecto estan, por ejemplo:

- `run_pueyo2024_paper_like_ab.py`
- `run_pueyo_best_of_sf_range_compact.py`
- `run_pueyo_low_full_workload.py`
- `run_pueyo_phy_ablation.py`
- `run_pueyo_paper_like_capture_sensitivity.py`
- `run_pueyo_workload_parity_audit.py`
- `run_pueyo_fig11_fig12_toa_bestof_campaign.py`
- `make_fig11_fig12_report_pack.py`

## 17.3 Rol de esta capa

Estos runners no redefinen el protocolo.  
Su rol actual es:

- parametrizar barridos,
- correr seeds,
- extender horizontes temporales,
- generar `results_runs.csv`, `results_agg.csv` o report packs,
- y consolidar resultados en `validation_results/`.

## 17.4 Distincion importante

La semantica del simulador vive en:

- `mesh_dv_baseline.cc`
- `MeshDvApp`
- `RoutingDv`
- `MeshLoraNetDevice`
- `SimpleGatewayLoraPhy`

La semantica **no** vive en los scripts de campana; estos solo configuran y repiten corridas.

---

# 18. Instrumentacion, hooks y validacion

## 18.1 Hooks de validacion presentes hoy

El codigo actual incluye instrumentacion util para auditoria y validacion:

- `PueyoValidationTrace`
- `EnableGapAuditTrace`
- `PueyoSyntheticEntriesNodeId`
- `PueyoSyntheticEntries`

## 18.2 Uso previsto

Estos hooks sirven para:

- auditar beacon payloads y decodificacion,
- inyectar entradas sinteticas para validar el beacon path Pueyo,
- medir cobertura de rutas y snapshots de control-plane,
- y generar evidencia de paridad con paper y runners de sensibilidad.

## 18.3 Estado funcional

Estos hooks siguen presentes en el codigo actual, pero **no** forman parte del comportamiento nominal del protocolo.  
Son soporte de validacion y deben documentarse como tal.

---

# 19. Limitaciones y supuestos actuales

## 19.1 Lo que el simulador modela hoy

- red mesh LoRa peer-to-peer estatica,
- single-channel,
- una sola recepcion activa a la vez,
- routing proactive DV,
- datos any-to-any o Pueyo all-to-all,
- comparacion entre baseline ToA y propuesta compuesta,
- sensibilidad a SF-range, receive-start, capture y workload.

## 19.2 Lo que no modela hoy

- multicanal,
- nodos con multi-demod tipo gateway LoRaWAN,
- movilidad,
- ARQ/ACK end-to-end,
- un network server LoRaWAN,
- una pila LoRaWAN completa.

## 19.3 Supuestos comparables actuales

El perfil `pueyo2024_paper_like` representa hoy la mejor aproximacion paper-like del workspace porque conserva:

- beacon path comparable Pueyo,
- `pueyo7b` para datos,
- `toa_only`,
- single-channel,
- single-demod,
- una sola reception path,
- `pueyo_fixed_capture`,
- receive-start FLoRa-like simplificado,
- `SF7-8` como rango comparable principal.

## 19.4 Discrepancias metodologicas que siguen siendo de runner

El perfil no garantiza por si solo:

- horizonte temporal full-workload en `low`,
- ni una campana concreta Fig. 11-12.

Eso sigue dependiendo del runner.

---

# 20. Mapa funcional del flujo principal

## 20.1 TX de beacon comparable

1. `mesh_dv_baseline.cc` configura `WireFormat=pueyo7b` y `PurePueyoBaselineMode=true` para el baseline comparable.
2. `MeshDvApp::BuildAndSendDv()` detecta `pueyo7b` y llama `BuildAndSendDvPueyo()`.
3. `RoutingDv::GetBestRoutesPueyo()` arma las rutas anunciables.
4. `SerializeDvEntriesPueyo()` serializa `destination + score`.
5. `BeaconWireHeaderPueyo` agrega `src`, `dst`, `flags_ttl`.
6. `ProcessTxQueue()` asigna el `rp_counter` justo antes del TX real.
7. `MeshLoraNetDevice::Send()` transmite usando `MeshMetricTag.sf` y `TxPowerDbm`.
8. `SimpleGatewayLoraPhy` gestiona la recepcion y `LoraInterferenceHelper` decide destruccion o entrega.

## 20.2 RX de beacon comparable

1. `MeshLoraNetDevice::Receive()` recupera `LoraTag` y, para beacons, sintetiza `from` desde el `src` logico.
2. `MeshDvApp::L2ReceiveV2()` parsea el beacon Pueyo.
3. Actualiza cache `nodeId -> linkAddr` y aprendizaje de SF observado.
4. Resuelve `linkSf`.
5. Extiende `rp_counter` a secuencia monotona por origen.
6. `DecodeDvEntriesPueyo()` reconstruye `destination + score`.
7. `RoutingDv::UpdateFromDvMsg()` actualiza `m_inboundRoutes`, `m_routes` y `m_backupRoutes`.

## 20.3 TX de datos `pueyo7b`

1. La app elige destino segun el workload.
2. Consulta `RoutingDv::GetRoute(dst)`.
3. Elige `sf` local del siguiente salto.
4. Resuelve `linkAddr` del `nextHop`.
5. Construye `DataWireHeaderPueyo7b`.
6. Encola y transmite via `MeshLoraNetDevice`.

## 20.4 Forwarding

1. Nodo intermedio recibe paquete.
2. Revalida TTL y evita backtracking inmediato si corresponde.
3. Reconsulta su propia tabla DV local.
4. Selecciona nuevo `nextHop` y `sf` local.
5. Reencola y retransmite.

---

# 21. Recomendacion documental para el proyecto

A partir del estado actual del codigo, la narrativa correcta del simulador es esta:

- `extended` existe, pero no es la referencia paper-like.
- `pueyo2024` es el baseline comparable estricto.
- `pueyo2024_paper_like` es la mejor aproximacion actual al paper para resultados comparables.
- El beacon comparable ya tiene path propio Pueyo.
- El data wire comparable sigue siendo `pueyo7b`.
- El modelo PHY sigue siendo single-channel y single-demod.
- La mejor paridad actual con Pueyo no se logra agregando multi-demod, sino manteniendo un solo canal y una sola recepcion activa, ajustando receive-start y SF-range.

---

# 22. Checklist de consistencia del documento

Este documento ya refleja el estado actual del codigo en estos puntos criticos:

- perfiles vigentes y sus contratos,
- beacon path Pueyo separado del V2 generico,
- `pueyo2024_paper_like` como perfil comparable principal,
- `pueyoFloraLikeRx` como receive-start simplificado y reversible,
- `pueyo7b` solo para data, con beacon comparable propio,
- `m_inboundRoutes` explicitas,
- `GetBestRoutesPueyo()` como politica baseline-only,
- `pueyo_fixed_capture` como modelo comparable actual,
- topologias `pueyo_grid` y `pueyo_random_equiv`,
- export de metricas y metadata actualizados,
- runners de campana presentes en el arbol.

---

# 23. Conclusion

El simulador actual ya no debe describirse como una implementacion generica heredada de `wire_v2` con un baseline comparable parcial.  
Hoy el proyecto tiene:

- un stack mesh propio y estabilizado,
- un beacon path comparable Pueyo explicito,
- un perfil `pueyo2024_paper_like` que concentra la mejor paridad actual con el paper,
- una capa de runners y auditorias que permiten separar semantica de protocolo de metodologia experimental,
- y una instrumentacion suficiente para defender el estado actual del simulador con evidencia de codigo y de runtime.

La referencia principal para comparaciones paper-like debe documentarse en adelante como:

- `pueyo2024_paper_like` para replica comparable principal,
- `pueyo2024` como baseline estricto historico/comparable,
- `proposal_pueyo_like` y `proposal_pueyo_like_observed` como perfiles de propuesta comparables sobre la misma base.

---

**Fin del FSD/LLD actualizado**
