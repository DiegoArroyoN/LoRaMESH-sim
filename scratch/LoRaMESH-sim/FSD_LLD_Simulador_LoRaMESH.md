# FSD/LLD Actualizado - LoRaMESH-sim

**Proyecto:** `LoRaMESH-sim`  
**Repositorio:** `ns-3-dev/scratch/LoRaMESH-sim/`  
**Fecha de actualizacion:** 2026-04-15  
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

## 1.1 Mapa documental

Este FSD ya no debe usarse como documento operativo unico.

La separacion correcta es:

- `README.md`
  - punto de entrada y quick-start
- `AI_OPERATOR_GUIDE.md`
  - guia operativa para otra IA o para trabajo remoto reproducible
- `FSD_LLD_Simulador_LoRaMESH.md`
  - arquitectura y dise?o del simulador

Si existe un handoff experimental remoto activo, debe leerse ademas como contexto operativo, pero no reemplaza ni este FSD ni la guia operativa.

## 1.2 Qu? no cubre este documento

Este FSD no debe usarse para:

- decidir por si solo qu? campa?a est? activa o consolidada
- reemplazar la gu?a operativa de otra IA
- asumir que una observacion experimental ya qued? validada si no fue registrada en el handoff o en la gu?a operativa

En particular:

- campa?as
- rutas de archivo consolidado
- criterio de cierre de una corrida
- y comandos de operaci?n remota

deben buscarse primero en `AI_OPERATOR_GUIDE.md`.

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

Antes de aplicar perfil y overrides por linea de comando, el entrypoint arranca con un conjunto de defaults crudos del binario.

Estos defaults de arranque:

- no deben confundirse con el baseline comparable efectivo,
- no representan por si solos un perfil experimental valido,
- y en la practica son sobreescritos por el perfil seleccionado y por los runners de campa?a.

Los defaults crudos actuales son:

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

Estos valores son solo la base de arranque del binario. El baseline operativo real se define recien despues de aplicar perfil, overrides y runner de campa?a.

## 5.2 Tabla de perfiles

| Perfil | Proposito | Wire de datos | Beacon path | Metrica DV | `sfLinkMode` | CSMA | Duty | Rango SF | Receive-start | Interferencia |
|---|---|---|---|---|---|---:|---:|---|---|---|
| `extended` | perfil legacy de ingenieria / experimentacion | `v2` | `v2` | `composite_score` | `observed_rxsf` | si | si | `7-12` | scan/lock por defecto | `puello` |
| `pueyo2024` | baseline comparable historico mas amplio | `pueyo7b` | Pueyo | `toa_only` | `deterministic_sensitivity` | no | no | `7-12` | scan/lock por defecto | `pueyo_fixed_capture` |
| `pueyo2024_paper_like` | mejor aproximacion actual al paper | `pueyo7b` | Pueyo | `toa_only` | `deterministic_sensitivity` | no | no | `7-8` | FLoRa-like immediate lock | `pueyo_fixed_capture` |
| `proposal_pueyo_like` | propuesta comparable contra Pueyo | `pueyo7b` | Pueyo | `composite_score` | `deterministic_sensitivity` | si | si (1%) | `7-12` | FLoRa-like immediate lock | `pueyo_fixed_capture` |
| `proposal_pueyo_like_observed` | propuesta comparable sin oraculo de SF | `pueyo7b` | Pueyo | `composite_score` | `observed_rxsf` | si | si (1%) | `7-12` | scan/lock por defecto | `pueyo_fixed_capture` |

Detalles operativos importantes:

- `proposal_pueyo_like`
  - `PrioritizeBeacons = false`
  - `PueyoStrictQueueScheduler = true`
- `proposal_pueyo_like_observed`
  - `PrioritizeBeacons = true`
  - `PueyoStrictQueueScheduler = false`

### Lectura correcta y trampas por perfil

| Perfil | Qu? se quiere medir | Qu? s? cambia | Qu? suele malinterpretarse |
|---|---|---|---|
| `pueyo2024_paper_like` | baseline comparable principal | `toa_only`, `SF7-8`, receive-start FLoRa-like, sin `CSMA`, sin duty | no es "el default del binario"; es un baseline configurado |
| `pueyo2024` | baseline comparable hist?rico m?s amplio | `toa_only`, `SF7-12`, scan/lock por defecto | no es la r?plica paper-like estricta actual |
| `proposal_pueyo_like` | bundle de propuesta comparable | `composite_score`, `CSMA`, duty `1%`, `SF7-12`, receive-start FLoRa-like | no a?sla por separado m?trica, MAC/duty y energ?a |
| `proposal_pueyo_like_observed` | variante comparable sin or?culo de SF | `observed_rxsf`, `CSMA`, duty `1%`, scan/lock por defecto | no es la variante principal de propuesta |
| `extended` | ingenier?a/legacy | stack general con defaults m?s amplios | no debe usarse como baseline paper-like |

Conclusi?n documental:

- `pueyo2024_paper_like` es el baseline real para campa?as comparables principales
- `proposal_pueyo_like` sirve para comparar bundle completo de propuesta vs baseline
- hoy el c?digo no ofrece perfiles limpios para atribuir por separado:
  - m?trica compuesta
  - MAC/duty
  - energ?a

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

`pueyo2024` es el baseline comparable historico mas amplio del codigo.  
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

Importante:

- la sombra actual no esta modelada como una perturbacion fija correlacionada por enlace
- hoy se implementa encadenando `RandomPropagationLossModel`
- por eso, con `shadowingSigmaDb > 0`, los umbrales de alcance dejan de ser cortes exactos y pasan a comportarse como una zona probabilistica

En un estudio controlado con `shadowingSigmaDb = 0`, los quiebres deterministas observados fueron:

- `SF7`: ultimo metro que funciona `224`, primer metro que falla `225`
- `SF8`: ultimo metro que funciona `313`, primer metro que falla `314`

Traducidos a diagonal equivalente de una grilla perfecta:

- `SF7`: `158.88 m`
- `SF8`: `221.46 m`

Por tanto, los pares `177/178` y `246/247/248` no representan los quiebres geometricos exactos de este modelo tal como esta implementado hoy.

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

En la practica actual:

- `pueyo2024` y `proposal_pueyo_like_observed` quedan en modo normal con `EnableSfScanRx=true`
- `pueyo2024_paper_like` y `proposal_pueyo_like` usan el modo FLoRa-like con `EnableSfScanRx=false`

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

## 7.7 Causa ra?z, mecanismo dominante y contadores

Lectura correcta del cuello PHY actual:

| Capa de interpretaci?n | Significado |
|---|---|
| causa ra?z | muchas transmisiones comparten el mismo medio single-channel |
| mecanismo dominante de fallo | un receptor ya est? ocupado demodulando otra se?al cuando llega una nueva |
| mecanismo secundario de fallo | una se?al ya lockeada muere por interferencia posterior |
| contadores m?s ?tiles | `rx_no_more_demodulators`, `rx_post_lock_interference_fail`, `pueyo_same_sf_overlap_events` |

Esto implica:

- el problema de fondo sigue siendo contenci?n del canal compartido
- pero el s?ntoma m?s da?ino en este modelo suele ser "receptor ocupado"
- por eso variantes temporales que espacian transmisiones pueden mejorar PDR sin tocar routing

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

Si `pueyoStrictQueueScheduler = true`, `ProcessTxQueue()` usa `SelectStrictQueueHead()` antes de decidir qu? paquete queda al frente de la cola.

Eso aplica tanto:

- con `CSMA = false`
- como con `CSMA = true`

La diferencia es el paso siguiente:

- con `CSMA = false`, el paquete elegido intenta salir directo
- con `CSMA = true`, el paquete elegido pasa despues por CAD/backoff/duty antes del TX real

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

Importante para interpretar el modelo actual:

- `CadDecisionModel = local_power` no declara busy por cualquier TX activa en el canal
- en la implementacion actual primero exige:
  - misma frecuencia
  - y mismo SF que el paquete que intenta salir
- solo despues compara potencia recibida contra sensibilidad CAD + margen

Entonces, el `CSMA/CAD` actual no representa una ocupacion universal del canal single-channel.  
Representa una decision local dependiente del SF del paquete en transmisi?n.

## 9.5 Control guard y latest-only

El stack actual conserva varias optimizaciones opcionales:

- `EnableControlGuard`
- `PrioritizeBeacons`
- `BeaconLatestOnly`
- `ExtraDvBeacon*`

Estas pueden ser utiles en perfiles generales o de propuesta, pero no forman parte del baseline comparable paper-like puro.

## 9.6 Qu? suele malinterpretarse en MAC/cola

- incluso con `CSMA = false`, sigue existiendo una cola TX local en la app
- por tanto, "paquete generado" no significa "paquete transmitido inmediatamente"
- `PueyoStrictQueueScheduler` no implica ausencia de cola; implica otra pol?tica de selecci?n del head
- `CadDecisionModel = local_power` no representa ocupaci?n universal del canal
- `dutyBlocked*` describe defer por pol?tica MAC, no p?rdida PHY

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
- por tanto, la semantica completa de deduplicacion de datos `pueyo7b` depende del tag interno local, no solo del header on-air de 7 bytes

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

Importante:

- en el path score-only comparable, la verdad on-air del beacon es `destination + score`
- la estructura interna `DvEntry` puede seguir cargando `hops`, `sf`, `toaUs` y otros campos legacy, pero esos no son verdad anunciada on-air en este path
- al recibir un beacon, esos campos se reconstruyen localmente usando el enlace hacia el originador y la metrica/configuracion local

## 10.5 Path de beacon real en perfiles comparables

Hoy, si `wireFormat == pueyo7b`:

- los **datos** usan `DataWireHeaderPueyo7b`,
- los **beacons** usan `BuildAndSendDvPueyo()`, `ParseBeaconWirePacketPueyo()` y `DecodeDvEntriesPueyo()`.

Esto ya no depende semanticamente del serializer generico V2 para el baseline comparable.
- el beacon comparable no publica `hops`, `sf` ni `toaUs` por entrada; esos quedan como reconstruccion local del receptor

## 10.6 Tabla de verdad on-air vs reconstrucci?n local

| Wire item | Vive on-air | Origen del valor | Qui?n lo reconstruye o usa localmente | Riesgo si se interpreta mal |
|---|---|---|---|---|
| `DataWireHeaderPueyo7b.src/dst/via/flags_ttl` | s? | app TX de datos | RX de datos y forwarding | correcto para identidad L3 m?nima |
| secuencia completa de datos `pueyo7b` | no en el header de 7 B | `MeshMetricTag` | app/collector local | creer que el header de 7 B basta para deduplicaci?n completa |
| `BeaconWireHeaderPueyo.src/dst/flags_ttl` | s? | app TX de beacon | RX de beacon | identidad del beacon y `rp_counter` truncado |
| `destination + score` de DV comparable | s? | `SerializeDvEntriesPueyo()` | `DecodeDvEntriesPueyo()` + `RoutingDv` | creer que hop/SF/ToA viajan expl?citos on-air |
| `hops`, `sf`, `toaUs`, `rawMetric` en `DvEntry` | no como verdad on-air comparable | reconstrucci?n local | `RoutingDv` y m?tricas | tratarlos como si hubieran sido anunciados por el vecino |
| `nextHopMac` | no | cache local `nodeId -> linkAddr` | app TX/forward | asumir que la ruta l?gica ya incluye direcci?n de enlace lista para usar |

## 10.7 `rp_counter` en beacons

En el wire no-v1, el lower 6-bit field del beacon se usa como `rp_counter`:

- se asigna justo antes del TX real en `ProcessTxQueue()`,
- avanza modulo 64,
- en recepcion se extiende a una secuencia monotona local por origen con `ResolveBeaconSequenceFromRpCounter()`.

## 10.8 Presupuesto real de beacon comparable

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

Importante:

- en el baseline comparable actual, `GetBeaconRouteCapacity()` usa primero `dvPayloadMaxBytes`
- eso significa que el limite practico del beacon comparable hoy viene del payload DV configurado, no del MTU del device
- el chequeo por MTU solo pasa a ser dominante si `dvPayloadMaxBytes <= 0`

## 10.9 MTU real en la ruta mesh

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

Lectura correcta de estas tres estructuras:

- `m_routes`
  - ruta primaria actualmente elegida para cada destino
- `m_backupRoutes`
  - candidato alternativo por destino, conservado para promotion local si la primaria expira o deja de ser usable
- `m_inboundRoutes`
  - reachability directa hacia nodos que efectivamente anunciaron un beacon recibido
  - no equivale por si sola a una ruta end-to-end arbitraria, pero si alimenta el candidate set que luego puede anunciarse o promoverse

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

Eso deja hoy dos capas relacionadas, pero no equivalentes:

- `CompositeMetric`
  - formula local fija
  - pesos `0.40 / 0.30 / 0.30`
  - penalizacion energetica con `p = 2`
- `RoutingDv`
  - formula authoritative para seleccionar y acumular rutas
  - pesos configurables:
    - `CompositeWToa`
    - `CompositeWHop`
    - `CompositeWEnergy`
  - curva energetica configurable:
    - `EnergyLo`
    - `EnergyHi`
    - `EnergyPow`
    - `EnergyMaxPenalty`

Por tanto, variar los pesos por CLI en `RoutingDv` no reconfigura automaticamente toda la logica del helper `CompositeMetric`.

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

Lectura mas fina por modo:

- `cost255`
  - el valor on-air crece con el costo cuantizado
  - menor valor anunciado = mejor ruta
- `score255`
  - el valor on-air es la inversion del costo cuantizado en rango `1..255`
  - mayor valor anunciado = mejor ruta
- `score100`
  - el valor on-air es una version normalizada en `1..100`
  - mayor valor anunciado = mejor ruta

Importante:

- el baseline comparable Pueyo usa `cost255`
- por tanto, en ese baseline un beacon con valor mas bajo representa mejor camino
- para `composite_score`, el costo crudo primero se cuantiza con `CompositeCostStep` y luego se codifica segun `CostEncoding`

## 11.4 Actualizacion al recibir un beacon

`UpdateFromDvMsg()` hace hoy este flujo:

1. valida origen, vecino, secuencia y cantidad de entradas,
2. construye una ruta directa al vecino emisor,
3. la guarda explicitamente tambien en `m_inboundRoutes`,
4. procesa cada `destination + score` anunciados,
5. para cada entrada arma un `candidate` usando el enlace local al originador,
6. pasa el candidato a `UpdateRoute()`.

Si el anuncio recibido llega como unreachable/poison:

- ese destino no entra como ruta usable,
- el estado previo puede marcarse como fallido/poisoned,
- y eso afecta tanto seleccion futura como elegibilidad para promotion de backups.

Matices importantes:

- la ruta directa al originador se intenta actualizar como cualquier otra, pero se bloquea si el destino esta en hold-down
- las entries remotas tambien se ignoran si el destino anunciado esta en hold-down
- en `toa_only`, el `score` on-air se decodifica a unidades de costo de camino y luego se suma el costo local del enlace
- en `composite_score`, el `score` on-air se decodifica a `pathRaw`, y sobre eso se suma el incremento local de enlace y la penalizacion energetica local
- por tanto, el anuncio recibido no se copia literal a la tabla: siempre se recompone un candidato local con el enlace hacia el originador del beacon

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

Mas fino:

- primero puede reservar capacidad para destinos activos
- luego puede forzar inclusion del sink si existe y aun no entro
- luego inserta `poison` con prioridad alta para invalidar rutas stale
- y finalmente completa el resto con seleccion top-score o sampleo estocastico sin reemplazo
- esta politica trabaja sobre `m_routes`; no es la politica baseline-only del path Pueyo

En la rama estocastica:

- `UNIFORM` usa pesos uniformes
- `COST_WEIGHTED` usa `1 / (0.05 + metrica_comparable)`
- la seleccion es sin reemplazo, porque cada ruta elegida se elimina del pool antes del siguiente draw

### `GetBestRoutesPueyo()`

Es la politica baseline-only del beacon comparable.  
Hoy hace esto:

- arma un candidate set a partir de `m_routes` y `m_inboundRoutes`,
- no usa prefases de destinos activos ni sink prioritario,
- si todo cabe, ordena por mejor costo,
- si no cabe, selecciona sin reemplazo con muestreo `cost_weighted` puro.

Lectura correcta frente a la politica generica:

- `GetBestRoutesPueyo()` no replica las prefases de destinos activos ni de sink
- tampoco introduce una fase separada de poison prioritario como la politica generica
- toma un candidate set mas cercano al baseline comparable y luego aplica orden por costo o muestreo `cost_weighted`
- por eso `PurePueyoBaselineMode=true` no solo cambia serializer; cambia tambien la politica concreta de que rutas salen en el beacon
- si una misma `destination` aparece en `m_routes` y `m_inboundRoutes`, primero se colapsa a un solo candidato por destino antes del anuncio

## 11.7 Regla de comparacion entre rutas

`IsCandidateBetter()` compara hoy por:

1. usabilidad (score 0 vs score > 0)
2. costo comparable (`GetComparableMetric()`)
3. SF mas bajo
4. desempate aleatorio si persiste empate y cambia next-hop

Matices importantes:

- el desempate aleatorio solo aparece si ya hubo empate total de usabilidad, costo y SF
- si el `nextHop` es el mismo, un empate no se trata como cambio real de ruta
- la hysteresis por mejora minima (`routeSwitchMinDeltaX100`) no vive en `IsCandidateBetter()`, sino mas abajo en `UpdateRoute()` y solo para `MetricMode::COMPOSITE_SCORE`

## 11.8 Expiracion, poison y limpieza

El routing actual soporta:

- timeout de ruta,
- poison announcements,
- purge,
- backup promotions,
- hold-down de destinos recientemente fallidos,
- y limites globales de tabla con eviction.

Lectura mas fina:

- `GetRoute()` primero intenta la primaria; solo si ya no es usable la elimina/poisoniza localmente y luego intenta promotion del backup
- la promotion de backup ocurre bajo demanda de lookup, no como tarea de fondo separada
- `poison` no representa una ruta valida de costo alto; representa explicitamente "destino no alcanzable" para ese anuncio
- un `candidate` con `scoreX100 = 0` solo envenena entradas existentes que compartan el mismo `nextHop`; no hace un borrado global indiscriminado del destino
- el hold-down se usa para frenar reintroduccion inmediata de destinos recientemente fallados
- `m_inboundRoutes` y `m_backupRoutes` no son solo cache pasiva; participan en la reconstruccion del candidate set y en la recuperacion local ante fallos
- el limite de backups hoy es practicamente **uno por destino**, porque `m_backupRoutes` es un `map<dest, RouteEntry>`
- ante overflow global de tabla, la eviction prioriza primero backups; solo si no hay backups pasa a expulsar primarias

## 11.9 Ciclo de vida real de una ruta

| Estado o transici?n | Qu? la dispara | Resultado real |
|---|---|---|
| nueva primaria | no existe primaria para ese destino | entra a `m_routes` |
| refresh misma primaria | llega candidato con mismo `nextHop` | se actualiza primaria existente |
| switch de primaria | candidato distinto mejora a la primaria y pasa hysteresis si aplica | primaria vieja puede pasar a backup |
| add backup | ya existe primaria, no hay backup y el candidato no reemplaza primaria | entra a `m_backupRoutes` |
| refresh backup | llega candidato con mismo `nextHop` que el backup | se actualiza backup existente |
| replace backup | candidato no reemplaza primaria pero s? mejora al backup | reemplaza backup |
| poison por `nextHop` | llega anuncio unreachable/poison o falla ligada a ese vecino | invalida entradas que usen ese `nextHop`, no borra globalmente el destino |
| expire/purge | timeout de ruta o limpieza de estado stale | salida de primaria o backup seg?n corresponda |
| backup promotion | `GetRoute()` encuentra primaria no usable y backup v?lido | backup asciende bajo demanda |

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

## 12.5 Qu? hace, qu? no hace y c?mo leerlo

Qu? hace hoy:

- desacopla el baseline comparable del serializer V2 gen?rico
- anuncia solo `destination + score`
- reconstruye localmente costo de camino y atributos auxiliares al recibir

Qu? no hace:

- no anuncia `hops`, `sf`, `toaUs` ni `rawMetric` como verdad on-air por entrada
- no convierte el beacon comparable en una copia wire-exacta del estado interno de `RoutingDv`

C?mo debe leerse:

- el beacon comparable es un canal de reachability cuantizada, no un volcado completo de la tabla de rutas
- la tabla local del receptor siempre es resultado de:
  - anuncio recibido
  - enlace local al originador
  - configuraci?n local de m?trica/codificaci?n
  - estado local de hold-down/backup/poison

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

## 13.7 Qu? hace, qu? no hace y c?mo leerlo

Qu? hace hoy:

- genera workload temporal peri?dico o agenda `pueyo_all_to_all`
- decide `nextHop` y `sf` localmente en cada TX o forward
- usa cola TX local incluso si `CSMA = false`
- puede descartar antes del primer TX si no logra materializar el siguiente salto

Qu? no hace:

- no garantiza por s? solo que todo paquete objetivo del workload llegue a generarse si el runner fija una ventana temporal insuficiente
- no usa el plan de forwarding del hop anterior como verdad obligatoria en el relay
- no implica ACK/ARQ extremo a extremo

C?mo debe leerse:

- `generated_count` significa "la app s? cre? ese paquete"
- `source_first_tx_count` significa "ese paquete s? ejecut? un primer TX", no "su primer hop fue exitoso"
- `drop_no_route_src` y `drop_no_route_relay` significan "no se pudo materializar el siguiente salto", no solo "no exist?a ruta l?gica"

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

`mesh_dv_baseline.cc` **solo** instala esta capa si:

- `enableNs3EnergyFramework = true`

Cuando eso ocurre, instala:

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

Cuando `enableNs3EnergyFramework = false`:

- no se instala `BasicEnergySource`
- no se instala `LoRaDeviceEnergyModel`
- el baseline comparable principal no depende de este backend para existir ni para correr

## 15.2 Uso funcional actual

La energia actual influye en:

- metricas y reportes,
- costo compuesto,
- consumo del dispositivo durante TX/RX/CAD/IDLE.

Lectura correcta:

- `loramesh::EnergyModel` interno sigue existiendo como parte de la logica mesh
- el Energy Framework de ns-3 es opcional
- por tanto, activar `enableNs3EnergyFramework` no significa "encender energia desde cero", sino cambiar el backend de depletion explicito del dispositivo

En el baseline comparable `toa_only`, la energia puede seguir registrandose, pero no gobierna la metrica principal de routing.

En particular:

- `pueyo2024_paper_like` no activa por si solo `enableNs3EnergyFramework`
- `proposal_pueyo_like` tampoco lo activa por defecto

Por eso no debe describirse el estado actual como una comparaci?n simple entre:

- baseline sin energia
- versus propuesta con Energy Framework ns-3

## 15.3 Qu? hace, qu? no hace y c?mo leerlo

Qu? hace hoy:

- registra y consume energ?a en la l?gica mesh interna
- permite que la energ?a entre al costo compuesto cuando la m?trica activa la usa
- puede instalar adem?s un backend ns-3 de depletion expl?cito si el perfil o CLI lo habilitan

Qu? no hace:

- no separa experimentalmente "energ?a" como dimensi?n limpia por s? sola en los perfiles actuales
- no convierte a `enableNs3EnergyFramework=true` en condici?n necesaria para que exista estado energ?tico utilizable

C?mo debe leerse:

- hoy "energ?a" significa una combinaci?n de:
  - estado energ?tico interno
  - reportabilidad
  - posible backend adicional del Energy Framework
- por tanto, cualquier comparaci?n de perfiles debe explicitar si est? comparando:
  - m?trica compuesta con t?rmino energ?tico
  - backend energ?tico
  - o ambos

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

Lectura importante:

- varias secciones repiten aliases historicos del mismo valor
- el resumen mezcla:
  - m?tricas de paquete generado/entregado,
  - m?tricas de intentos TX,
  - y contadores de eventos PHY/control-plane
- por tanto, no todo campo llamado `ratio` o `pdr` debe leerse como "porcentaje de paquetes entregados"

## 16.4 Definiciones importantes actuales

El resumen exporta explicitamente:

- `delivery_ratio`
- `pdr`
- `pdr_post_convergence`
- `pdr_no_drain`
- `source_first_tx_count`
- `source_first_tx_ratio`
- `delivered_per_tx_attempt`
- `tx_attempts_per_generated`
- `admission_ratio`
- `source_admission_ratio`
- `forwarded_unique_count`
- `routes_total`
- `control_tx_sent`
- `data_tx_sent`
- `rx_no_more_demodulators`
- `rx_scan_miss_before_lock`
- `rx_post_lock_interference_fail`
- `pueyo_same_sf_overlap_events`
- `queued_packets_end`

Esto permite separar:

- generacion,
- admision a TX,
- forwarding efectivo,
- entrega final,
- perdidas de control-plane,
- backlog,
- y perdidas PHY.

Para evitar ambig?edad:

- el PDR baseline que se reporta en campa?as es `delivery_ratio`
- `pdr` y `delivery_ratio` son equivalentes en el resumen actual
- `delivery_ratio = delivered_count / generated_count`
- ese denominador considera solo paquetes de datos realmente generados
- los beacons no entran al PDR
- si una corrida tuvo `generated_count < workload_target`, eso es un artefacto temporal de generaci?n, no una p?rdida PHY
- dentro de la secci?n `pdr`, el campo `total_data_tx` hoy **no** representa intentos TX reales
- en la implementaci?n actual `pdr.total_data_tx` se exporta con el mismo valor que `total_data_generated`
- los intentos TX reales de datos est?n en:
  - `pdr.legacy_total_data_tx_attempts`
  - `tx_attempts.total_data_tx_attempts`

Otras definiciones ?tiles:

- `source_first_tx_count`
  - paquetes generados que s? alcanzaron al menos un primer TX desde origen
  - no significa que el primer hop haya sido recibido con ?xito
  - hoy depende de que exista una marca en `m_firstTxTime`, es decir, de que el primer TX haya llegado a ejecutarse en el dispositivo
- `source_first_tx_ratio`
  - alias normalizado de `source_first_tx_count / generated_count`
- `tx_attempts_per_generated`
  - intentos TX de datos por paquete generado
- `admission_ratio`
  - alias del mismo `tx_attempts_per_generated` en el resumen actual
- `source_admission_ratio`
  - alias del mismo `source_first_tx_ratio` en el resumen actual
  - no representa una metrica independiente adicional
- `drop_no_route_src`
  - paquete generado pero sin ruta utilizable en el origen
- `drop_no_route_relay`
  - paquete que ya sali? del origen pero muri? en un relay antes del siguiente TX
  - en la instrumentacion actual mezcla:
    - ausencia de ruta utilizable en el relay
    - y ausencia de `linkAddr` utilizable para el `nextHop` del relay
- `pdr_no_drain`
  - PDR medido solo sobre paquetes generados antes de `dataStopSec` y entregados antes de `dataStopSec`
  - sirve para aislar el efecto del drain posterior al fin de la fase de generaci?n
- `legacy_pdr_tx_based`
  - `delivered / totalDataTxLegacy`
  - no es el PDR baseline usado en campa?as
  - sirve como metrica historica basada en intentos TX, no en workload generado
- `pdr_by_source`
  - para cada `src`, usa `delivered_by_src / generated_by_src`
  - si un origen no gener? trafico, no aparece
- `delivery_by_destination`
  - lista solo conteos absolutos entregados por `dst`
  - no incluye denominador ni PDR por destino
- `throughput.throughput_bps`
  - usa `totalDataTxLegacy * payloadBits / activeTrafficSec`
  - por tanto representa carga agregada intentada de datos, no goodput entregado
- `throughput.goodput_bps`
  - usa `deliveredPackets * payloadBits / activeTrafficSec`
  - esta s? es la tasa ?til efectivamente entregada

Importante para interpretar diagnosticos PHY:

- `rx_no_more_demodulators`
- `rx_post_lock_interference_fail`
- `pueyo_same_sf_overlap_events`

no son una particion paquete-a-paquete de las p?rdidas.  
Son contadores de **eventos PHY** acumulados en la corrida.

Consecuencias:

- pueden ser mayores que `generated_count`
- no deben leerse como porcentaje directo de paquetes perdidos
- sirven para diagnosticar el regimen de presi?n del canal y del receptor, no para repartir causalmente el 100% del PDR perdido

## 16.5 Cuantizacion y export de score

La secci?n `quantization` no exporta solo "qu? score anunci? el nodo", sino un muestreo de c?mo el costo crudo local termina cuantizado.

Campos y lectura correcta:

- `metric_raw_*`
  - resumen estad?stico del costo crudo positivo observado en `m_quantizationSamples`
- `toa_cost_raw_*`
  - alias exportado con el mismo valor que `metric_raw_*`
  - el nombre es historico y puede ser enga?oso en `composite_score`
  - en `composite_score` no debe leerse como "ToA puro"; sigue siendo el costo crudo cuantizable del modo activo
- `score_quantized_*`
  - resumen del score/costo ya cuantizado y listo para codificaci?n on-air
- `quantization_collisions`
  - cuenta cu?ntos valores crudos distintos (`rawMetricMilli`) colapsaron al mismo `scoreQuantized`
  - no mide colisiones de paquetes ni colisiones PHY
- `quantization_collision_ratio`
  - `quantization_collisions / samples`
- `saturation_count`
  - cuenta cu?ntas muestras alcanzaron o excedieron el valor m?ximo representable por la codificaci?n vigente
  - el umbral usa:
    - `255` para `cost255` y `score255`
    - `100` para `score100`
  - en `composite_score`, adem?s considera `CompositeCostStep`
- `saturation_ratio`
  - `saturation_count / samples`
- `sample_top_raw`
  - no es "top best routes"
  - es una muestra ordenada por `rawMetric` ascendente y luego por `scoreQuantized`
  - sirve para inspeccionar cuantizaci?n, no para reconstruir toda la tabla de routing

Interpretaci?n correcta:

- una `quantization_collision` significa p?rdida de resoluci?n num?rica, no p?rdida de paquetes
- una `saturation_count` alta sugiere que el rango on-air se est? quedando corto para el costo efectivo observado
- estas m?tricas ayudan a auditar fidelidad de anuncio y orden relativo de rutas, no el PDR directamente

## 16.6 Duplicaciones y aliases a no sobreinterpretar

El resumen actual conserva varios aliases por compatibilidad o conveniencia anal?tica:

- `pdr.pdr` == `pdr.delivery_ratio`
- `pdr.tx_attempts_per_generated` == `pdr.admission_ratio`
- `pdr.source_first_tx_ratio` == `pdr.source_admission_ratio`
- `tx_attempts.attempts_per_generated` == `tx_attempts.tx_attempts_per_generated` == `tx_attempts.admission_ratio`
- `tx_attempts.source_first_tx_ratio` == `tx_attempts.source_admission_ratio`
- `control_plane.beacon_tx_sent` == `control_plane.control_tx_sent`
- `queue_backlog.cad_busy_events` duplica el mismo acumulado tambi?n visible en `control_plane`
- `queue_backlog.duty_blocked_events` complementa a `control_plane.duty_blocked_control/data`, no los reemplaza

Una IA que lea el JSON no deber?a contar estos aliases como m?tricas independientes.

`pdrEndWindowSec` no redefine esta m?trica base. Se usa para vistas derivadas como:

- `pdr_post_convergence`
- y otros cortes temporales exportados en el resumen

## 16.7 Tabla de lectura m?nima del JSON

| Campo JSON | Sem?ntica real |
|---|---|
| `pdr.delivery_ratio` | PDR baseline real: entregados / generados |
| `pdr.pdr` | alias de `delivery_ratio` |
| `pdr.total_data_generated` | workload realmente generado |
| `pdr.total_data_tx` | alias hist?rico; hoy no son intentos TX reales |
| `pdr.legacy_total_data_tx_attempts` | total de intentos TX de datos |
| `pdr.source_first_tx_count` | paquetes generados que s? ejecutaron al menos un primer TX |
| `tx_attempts.total_data_tx_attempts` | intentos TX de datos |
| `forwarding.forwarded_unique_count` | paquetes ?nicos que s? fueron reenviados al menos una vez |
| `control_plane.rx_no_more_demodulators` | eventos donde una nueva llegada no pudo adquirirse por receptor ocupado |
| `control_plane.rx_post_lock_interference_fail` | eventos donde una recepci?n ya lockeada fall? por interferencia posterior |
| `quantization.quantization_collisions` | colisiones de representaci?n num?rica, no de paquetes |
| `queue_backlog.queued_packets_end` | paquetes a?n pendientes al final de la corrida |
| `drops.drop_no_route_src` | ca?da en origen por falta de ruta usable o de `linkAddr` usable |
| `drops.drop_no_route_relay` | ca?da en relay por falta de ruta usable o de `linkAddr` usable |

## 16.8 Tabla de unidades correctas

| Campo o familia | Unidad correcta |
|---|---|
| `delivery_ratio`, `pdr`, `pdr_no_drain`, `pdr_post_convergence` | fracci?n `0..1` |
| `*_count`, `*_total`, `delivered`, `generated`, `queued_packets_end` | conteo |
| `rx_no_more_demodulators`, `rx_post_lock_interference_fail`, `pueyo_same_sf_overlap_events` | eventos por corrida |
| `throughput_bps`, `goodput_bps` | bits por segundo |
| `*_delay_*_s`, `*_time_s_*`, `active_traffic_sec` | segundos |
| `tx_attempts_per_generated`, `admission_ratio` | intentos por paquete generado |
| `source_first_tx_ratio`, `source_admission_ratio` | fracci?n de paquetes generados |
| `score_quantized_*` | valor cuantizado adimensional |
| `metric_raw_*`, `toa_cost_raw_*` | costo crudo adimensional del modo activo |

---

# 17. Runners y capa de campanas

## 17.1 Entry point base

La simulacion base sigue entrando por:

- `ns-3-dev/scratch/LoRaMESH-sim/mesh_dv_baseline.cc`

## 17.2 Runners de campana presentes en el arbol

El directorio `scratch/LoRaMESH-sim/` contiene runners Python para campa?as y auditorias.  
Entre los que hoy forman parte del estado real del proyecto estan, por ejemplo:

- `run_pueyo2024_paper_like_ab.py`
- `run_pueyo_best_of_sf_range_compact.py`
- `run_pueyo_low_full_workload.py`
- `run_pueyo_phy_ablation.py`
- `run_pueyo_paper_like_capture_sensitivity.py`
- `run_pueyo_workload_parity_audit.py`
- `run_pueyo_fig11_fig12_toa_bestof_campaign.py`
- `make_fig11_fig12_report_pack.py`

Importante:

- esta lista no es exhaustiva
- en la operacion real del proyecto tambien se han usado runners externos al arbol del repo y luego copiados al remoto
- por tanto, la metodologia experimental vigente no debe inferirse solo desde los scripts presentes dentro de `scratch/LoRaMESH-sim/`

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

Matiz importante:

- `PueyoValidationTrace` existe tanto en `MeshDvApp` como en `RoutingDv`
- por tanto, al activarlo no solo se audita serializacion/parsing del beacon path Pueyo, sino tambien parte de la logica de actualizacion y seleccion de rutas
- `PueyoSyntheticEntries*` en cambio pertenece al beacon path de la app y no redefine el algoritmo nominal de routing

## 18.2 Uso previsto

Estos hooks sirven para:

- auditar beacon payloads y decodificacion,
- inyectar entradas sinteticas para validar el beacon path Pueyo,
- medir cobertura de rutas y snapshots de control-plane,
- y generar evidencia de paridad con paper y runners de sensibilidad.

## 18.3 Estado funcional

Estos hooks siguen presentes en el codigo actual, pero **no** forman parte del comportamiento nominal del protocolo.  
Son soporte de validacion y deben documentarse como tal.

## 18.4 Qu? no debe inferirse desde los hooks

- activar `PueyoValidationTrace` no redefine la pol?tica nominal de routing
- `PueyoSyntheticEntries*` no describe tr?fico o anuncios normales del baseline; describe un camino de prueba
- la existencia de un hook no implica que esa sem?ntica est? activa en campa?as consolidadas

C?mo debe leerse:

- hooks = soporte de auditor?a
- baseline = comportamiento sin instrumentaci?n extraordinaria

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

## 19.5 Limites de interpretaci?n experimental

- una ca?da de PDR no debe atribuirse autom?ticamente a routing si los contadores PHY dominan
- un cambio de `1 m` en distancia no genera un salto duro si el modelo activo usa `shadowingSigmaDb > 0`
- campa?as con mismo perfil pero distinta geometr?a no a?slan una sola causa f?sica
- campa?as con `proposal_pueyo_like` no a?slan una sola causa algor?tmica porque cambian varias dimensiones a la vez

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

Matices importantes:

- el paquete cuenta como `generated` antes de consultar la ruta
- si `GetRoute(dst)` devuelve `nullptr`, el paquete cae como `drop_no_route_src`
- si existe ruta logica pero no se puede resolver una `linkAddr` usable para el `nextHop`, tambien cae como `drop_no_route_src`
- por tanto, en la instrumentacion actual `drop_no_route_src` mezcla:
  - ausencia de ruta DV usable en el origen
  - y ausencia de direccion de enlace utilizable para el siguiente salto
- solo si ambas cosas pasan, ruta y `linkAddr`, el paquete entra al camino de `SendWithCSMA()` y recien ahi puede contribuir a `source_first_tx_count`

## 20.4 Forwarding

1. Nodo intermedio recibe paquete.
2. Revalida TTL y evita backtracking inmediato si corresponde.
3. Reconsulta su propia tabla DV local.
4. Selecciona nuevo `nextHop` y `sf` local.
5. Reencola y retransmite.

Matices importantes:

- el relay vuelve a consultar `GetRoute(dst)` localmente; no reusa ciegamente el plan del hop anterior
- si el relay no tiene ruta usable, el paquete cae como `drop_no_route_relay`
- si hay ruta logica pero el relay no puede resolver una `linkAddr` usable para ese `nextHop`, tambien cae como `drop_no_route_relay`
- por tanto, igual que en origen, `drop_no_route_relay` no significa exclusivamente "sin ruta DV"; significa "el relay no pudo materializar el siguiente salto"
- solo despues de superar ruta y `linkAddr` el paquete vuelve a `SendWithCSMA()` y entra al regimen normal de cola / duty / CAD / PHY

---

# 21. Errores de interpretacion frecuentes

Errores que el FSD ya no deber?a permitir:

- confundir `generated_count < workload_target` con p?rdida PHY
- leer contadores PHY como porcentaje directo de paquetes perdidos
- asumir que `drop_no_route_src` significa solo "sin ruta DV"
- asumir que `drop_no_route_relay` significa solo "sin ruta DV"
- asumir que `source_first_tx_ratio` implica recepci?n exitosa del primer hop
- inferir sem?ntica del protocolo desde runners o scripts de campa?a
- tratar `177/248` como umbrales geom?tricos exactos del modelo actual
- contar aliases del JSON como si fueran m?tricas independientes
- leer `pdr.total_data_tx` como intentos TX reales
- leer `quantization_collisions` como colisiones de paquetes

---

# 22. Recomendacion documental para el proyecto

A partir del estado actual del codigo, la narrativa correcta del simulador es esta:

- `extended` existe, pero no es la referencia paper-like.
- `pueyo2024` es un baseline comparable historico mas amplio, no la replica paper-like estricta actual.
- `pueyo2024_paper_like` es la mejor aproximacion actual al paper para resultados comparables.
- El beacon comparable ya tiene path propio Pueyo.
- El data wire comparable sigue siendo `pueyo7b`.
- El modelo PHY sigue siendo single-channel y single-demod.
- La mejor paridad actual con Pueyo no se logra agregando multi-demod, sino manteniendo un solo canal y una sola recepcion activa, ajustando receive-start y SF-range.

---

# 23. Checklist de consistencia del documento

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

# 24. Conclusion

El simulador actual ya no debe describirse como una implementacion generica heredada de `wire_v2` con un baseline comparable parcial.  
Hoy el proyecto tiene:

- un stack mesh propio y estabilizado,
- un beacon path comparable Pueyo explicito,
- un perfil `pueyo2024_paper_like` que concentra la mejor paridad actual con el paper,
- una capa de runners y auditorias que permiten separar semantica de protocolo de metodologia experimental,
- y una instrumentacion suficiente para defender el estado actual del simulador con evidencia de codigo y de runtime.

La referencia principal para comparaciones paper-like debe documentarse en adelante como:

- `pueyo2024_paper_like` para replica comparable principal,
- `pueyo2024` como baseline historico/comparable mas amplio,
- `proposal_pueyo_like` y `proposal_pueyo_like_observed` como perfiles de propuesta comparables sobre la misma base.

---

**Fin del FSD/LLD actualizado**

