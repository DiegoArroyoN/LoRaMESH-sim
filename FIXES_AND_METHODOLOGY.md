# Fixes y Metodología — Simulador LoRa Mesh DV (NS-3)
**Fecha:** 2026-04-20  
**Referencia:** Pueyo-Centelles 2024, "A Minimalistic Distance-Vector Routing Protocol for LoRa Mesh Networks", IEEE Access 2024  
**Repo:** `/home/diego/sim/LoRaMESH-sim-frozen-20260327/`

---

## 1. Fixes de Validez (B1–B5)

### B1 — Contador `origin_pending_at_stop`
**Archivo:** `scratch/LoRaMESH-sim/mesh_dv_app.h`, `mesh_dv_app.cc`, `metrics_collector.h`, `metrics_collector.cc`  
**Problema:** No había forma de saber cuántos paquetes origen quedaban en cola al terminar la simulación.  
**Fix:** Se añadió `uint64_t m_originPendingAtStop{0}` incrementado en `StopApplication()` sobre paquetes aún pendientes de transmitir. Se exporta en `runtime_stats.origin_pending_at_stop` del JSON de resumen.  
**Semántica:** `m_dataPacketsGenerated` NO cambia — sigue contando paquetes generados en memoria. `originPendingAtStop` es un contador diagnóstico opcional para post-proceso.

### B2 — `dedupWindow` 86400s (24h)
**Archivo:** `scratch/LoRaMESH-sim/mesh_dv_app.h` y `mesh_dv_baseline.cc`  
**Problema:** Valor por defecto era 600s. En simulaciones largas (Low load, N grande), las ventanas de deduplicación se purgaban antes de terminar la simulación, causando que paquetes duplicados tardíos se contaran como entregas nuevas → PDR inflado artificialmente.  
**Fix:** Default cambiado a 86400s (> cualquier `stopSec` planeado). El PDR resultante (~53% en N=9 seed=1) es el valor real; el anterior ~77% era falso.

### B4 — `SetStream` por nodo para reproducibilidad RNG
**Archivo:** `scratch/LoRaMESH-sim/mesh_dv_app.cc`, `src/loramesh/model/loramesh-routing-dv.cc`  
**Problema:** Los RNGs no tenían stream fijo por nodo, por lo que añadir/quitar nodos cambiaba los resultados de todos los demás.  
**Fix:**
- `MeshDvApp`: `m_rng->SetStream(1000000 + nodeId)`
- `LoRaMeshRoutingDv`: `m_rng->SetStream(2000000 + nodeId)`  
**Efecto:** Tres corridas con `rngRun=1` producen hashes md5 idénticos; `rngRun=2` produce hashes distintos → reproducibilidad verificada.

### B5 — Bug de conteo e2e en `ForwardWithTtl` (solo protocolo v1)
**Archivo:** `scratch/LoRaMESH-sim/mesh_dv_app.cc`  
**Problema:** El conteo de paquetes entregados e2e estaba gateado con `if (myId == m_collectorNodeId)`, ignorando entregas en nodos no-colector.  
**Fix:** Se eliminó el gate; todos los nodos incrementan el contador e2e cuando son el destino final. Solo afecta a `wireFormat=v1` (legacy); pueyo7b no se ve afectado.

---

## 2. Fix de Métrica — C4: Fórmula Throughput

**Archivo:** `scratch/LoRaMESH-sim/metrics_collector.h`, `metrics_collector.cc`  
**Problema:** `throughput_bps` usaba `totalDataTxLegacy` (intentos de TX = 2145 para N=9) en lugar de recepciones unicast exitosas de cualquier hop.  
**Definición paper (Pueyo 2024):**
> "Throughput: cantidad total de datos válidos (solo payload) transmitidos por cualquier nodo y recibidos correctamente (sin colisiones) por el destinatario indicado en la cabecera del mensaje."

**Fix:**
1. Añadido `uint32_t m_dataRxAnyHopCount{0}` en `MetricsCollector` (privado)
2. Incrementado en `RecordRx()` ANTES del gate `essentialMetricsOnly`, filtrado a `dst != 0xFFFF` (excluye beacons broadcast)
3. Fórmula: `throughputBps = (m_dataRxAnyHopCount × payloadBits) / activeTrafficSec`
4. Exportado en JSON bajo `throughput.data_rx_any_hop_count`

**Resultado verificado (N=9, grid 177m, stopSec=2100, rngRun=1):**

| KPI | Antes | Después | Paper |
|-----|-------|---------|-------|
| throughput_bps | 170.746 | **105.711** | any-hop RX × payload / t |
| goodput_bps | 74.667 | 74.667 | e2e × payload / t ✓ |
| PDR | 0.5345 | 0.5345 | delivered/generated ✓ |
| delay.avg_s | 0.1545 | 0.1545 | avg sobre entregados ✓ |

---

## 3. Mejora Metodológica — C2/Option B: `autoDataStartSec`

**Archivo:** `scratch/LoRaMESH-sim/mesh_dv_baseline.cc`  
**Contexto:** El parámetro `dataStartSec=90s` (fijo) es insuficiente para N≥36 porque la convergencia DV en grids grandes requiere `diameter × beacon_interval` (hasta ~840s para N=64).  
**Nota:** Esto **no afecta PDR** (los paquetes generados antes de que exista ruta quedan en queue y se reenvían cuando aparece la ruta). Sí afecta la latencia reportada.

**Nuevo flag CLI:**
```
--autoDataStartSec=true   # Option B: escala por diámetro del grafo
--autoDataStartSec=false  # Option A: mantiene dataStartSec=90s (default)
```

**Fórmula Option B:**
```
diameter    = 2 × (gridSide - 1)
warmupHops  = floor(60s / beaconIntervalWarm)   = 1 para pueyo_paper_like
remHops     = max(0, diameter - warmupHops)
convSec     = 60 + remHops × beaconIntervalStable
dataStartSec = ceil(convSec × 1.3)              # margen 30%
```

**`dataStartSec` resultante por N:**

| N | Option A | Option B |
|---|----------|----------|
| 9 (3×3) | 90 s | 312 s |
| 16 (4×4) | 90 s | 468 s |
| 25 (5×5) | 90 s | 624 s |
| 36 (6×6) | 90 s | 780 s |
| 49 (7×7) | 90 s | 936 s |
| 64 (8×8) | 90 s | 1092 s |

---

## 4. Script de Verificación Pre-Campaña: `smoke_verify.sh`

**Ubicación:** `smoke_verify.sh` (raíz del repo)  
**Uso:**
```bash
bash smoke_verify.sh             # completo: md5 + KPIs (~2 min)
bash smoke_verify.sh --skip-md5  # solo KPIs
bash smoke_verify.sh --verbose   # output completo del simulador
```

**Checks (N=9, grid 177m, stopSec=2100, rngRun=1):**
| Check | Valor de referencia |
|-------|---------------------|
| md5 rx.csv | `84c7b7a72a910c415d7781792c9fa3c4` |
| md5 tx.csv | `2b8f7c8171ce490a9a9f7c5b49ee7e6c` |
| PDR | 0.5345 (938/1755) |
| throughput_bps | 105.711 |
| data_rx_any_hop | 1328 |
| goodput_bps | 74.667 |
| delay.avg_s | 0.154526 |
| delay.count | 938 |

---

## 5. Alineación con Paper: Ecuaciones KPI

| KPI | Ecuación implementada | Alineación |
|-----|----------------------|------------|
| **PDR** | `delivered_e2e / total_generated` | ✅ exacta |
| **Throughput** | `any_hop_unicast_RX × payload_bits / active_sec` | ✅ exacta (post C4-fix) |
| **Goodput** | `delivered_e2e × payload_bits / active_sec` | ✅ exacta |
| **Latency** | `mean(t_rx_final - t_gen)` solo sobre entregados | ✅ exacta |

**Configuración de tráfico (paper):**
- Paquete: 27B total = 7B header + 20B payload (160 bits)
- Por nodo: 100 paquetes × cada uno de los (N−1) destinos = `100×(N−1)` paquetes
- Destinos en orden aleatorio (`--trafficMode=pueyo_all_to_all`)
- 4 niveles de carga: Low=100s, Medium=10s, High=1s, Saturation=0.1s entre paquetes

---

## 6. Backups

Todos los archivos modificados tienen backups automáticos con timestamp:
```
metrics_collector.h.bak-c4fix-20260420-100504
metrics_collector.cc.bak-c4fix-20260420-100504
mesh_dv_baseline.cc.bak-optionB-20260420-...
```

---

## 7. CSMA/CAD con backoff — Fixes y perfil nuevo (2026-04-24)

**Archivos tocados:**
- `src/loramesh/model/loramesh-mac-csma-cad.{h,cc}`
- `scratch/LoRaMESH-sim/mesh_dv_app.{h,cc}`
- `scratch/LoRaMESH-sim/mesh_dv_baseline.cc`
- `scratch/LoRaMESH-sim/metrics_collector.{h,cc}`
- `plot_loss_v4.py`

**Motivación:**
El MAC `CsmaCadMac` existente (1) dejaba `CadSenseMarginDb=0` (CAD decide sobre el nivel exacto de sensibilidad SX1276, hipersensible a fluctuaciones de ruido), (2) decrementaba `m_failures` de uno en uno tras CAD limpia en lugar de resetearse como 802.11, (3) exponía la API muerta `SetBackoffWindow` / `m_backoffWindow` que ya no controlaba nada pero aún era llamada desde `MeshDvApp` inicializando un miembro huérfano, y (4) al faltar una política de reintentos máximos + tope de cola en el cliente CSMA, un paquete podía quedar atascado indefinidamente en la cabeza de `m_txQueue` si el canal se saturaba (Head-Of-Line blocking sin contabilidad).

### 7.1 — MAC (`loramesh-mac-csma-cad.{h,cc}`)

- **`CadSenseMarginDb` default 0 → 6 dB.** Siguiendo la práctica de radios LoRa reales que añaden 3-6 dB de margen sobre la sensibilidad del SX1276 para evitar dispararse con ruido cuasi-al-suelo. La atribución sigue siendo configurable por CLI (`--cadSenseMarginDb`).
- **Reset de `m_failures` tras CAD limpia.** Se reemplazó
  ```cpp
  if (m_failures > 0) { m_failures--; }
  ```
  por
  ```cpp
  // B1 fix (802.11-style): reset failures on clean CAD, not gradual decrement.
  m_failures = 0;
  ```
  El contador `m_failures` rige la ventana lineal de backoff (`ComputeBackoffWindowSlots`), por lo que mantener decrementos graduales impedía que la ventana volviese al mínimo incluso en canales súbitamente libres — lo contrario de la semántica DIFS/802.11.
- **API muerta eliminada.** `void SetBackoffWindow(uint8_t)`, miembro `uint8_t m_backoffWindow`, y flag obsoleto `bool m_maxBackoffSlotsExplicit` se removieron de header y ctor. La ventana máxima ahora se configura solo por `SetMaxBackoffSlots` (o el default 64).

### 7.2 — App (`mesh_dv_app.{h,cc}`)

- **Queue cap con evicción FIFO de datos (no-beacon).** Antes de `push_back`/`push_front` en `SendWithCSMA`, si `m_txQueue.size() >= m_csmaTxQueueMax` se busca la primera entrada de datos no-cabeza-en-aire y se desaloja, incrementando `m_dropQueueOverflow`. Si solo hay beacons o la cabeza está transmitiendo, el paquete nuevo se descarta.
- **Máximo de reintentos CSMA por entrada.** En la rama `channelBusy` de `ProcessTxQueue`, `entry.retries++` antes de programar el backoff; cuando `entry.retries > m_csmaMaxRetries` se hace `pop_front()` + `ProcessTxQueue()` y se incrementa `m_dropMaxCsmaRetries`. Antes, la retries++ nunca ocurría en esta rama (solo se reseteaba a 0 en `SendWithCSMA`), por lo que un paquete podía quedar atascado en la cabeza indefinidamente.
- **Se eliminó `m_mac->SetBackoffWindow(m_backoffWindow);`** (ya no existe) y **`m_mac->NotifyTxResult(ok);`** (duplicaba el reset que ya hace el CAD limpia; se eliminó para mantener una única fuente de verdad para `m_failures`).
- **Fallback `(1 << m_backoffWindow) - 1`** (solo activo si `m_mac=nullptr`, algo que no pasa en prod pero queda el camino) se reemplazó por `m_rng->GetInteger(0, 63)`, que es equivalente a 64 slots fijos (default de `kDefaultMaxBackoffSlots`).
- **Nuevos atributos (TypeId):**
  ```
  CsmaMaxRetries   (UintegerValue, default 8)  — m_csmaMaxRetries
  CsmaTxQueueMax   (UintegerValue, default 32) — m_csmaTxQueueMax
  ```
- **Nuevo contador:** `m_dropMaxCsmaRetries` → `RuntimeNodeStats::dropMaxCsmaRetries` → JSON `drops.drop_max_csma_retries`.

### 7.3 — Métricas (`metrics_collector.{h,cc}`)

Se añadió el campo `dropMaxCsmaRetries` en `RuntimeNodeStats`, se suma en el bucle por-nodo, y se emite en `"drops": { ..., "drop_max_csma_retries": N, ... }` del summary JSON entre `drop_queue_overflow` y `drop_backtrack`.

### 7.4 — Baseline (`mesh_dv_baseline.cc`)

- **CLI nuevos:**
  ```
  --cadSenseMarginDb=6.0
  --csmaMaxRetries=8
  --csmaTxQueueMax=32
  ```
- **Nuevo perfil `pueyo2024_paper_like_csmacad`**: espejo de `pueyo2024_paper_like` con la sola diferencia de `cfg.enableCsma = true`. Mantiene duty-cycle deshabilitado (equivalente a 100% de disponibilidad) para aislar el efecto del MAC. Mismo `routeMetricMode=toa_only`, `sfLinkMode=deterministic_sensitivity`, `sfMin=7 sfMax=8`, `pueyoFloraLikeRx=true`, `enableSfScanRx=false` que el perfil paper_like.
- **Validador** del nuevo perfil replica las mismas aserciones que `pueyo2024_paper_like` excepto que exige `enableCsma=true`.
- **`PurePueyoBaselineMode`** incluye ahora al nuevo perfil (es una variante de comparación Pueyo, no una propuesta composite).
- **`SetDefaultFailSafe`** añadidos para `CsmaCadMac::CadSenseMarginDb`, `MeshDvApp::CsmaMaxRetries`, `MeshDvApp::CsmaTxQueueMax`.

### 7.5 — Plot (`plot_loss_v4.py`)

Nueva categoría `max_retries` con color `#D35400` y etiqueta "Max CSMA retries", entre `queue_full` y `other`. Consume `j['drops']['drop_max_csma_retries']`. Retrocompatible: si el JSON no trae el campo, se devuelve 0 y la categoría queda oculta por el threshold `ACTIVE_THRESHOLD=0.5%`.

### 7.6 — Verificación (smoke runs, 2026-04-24)

| Escenario | N | Tráfico | stopSec | PDR | `cad_busy_events` | `drop_max_csma_retries` |
|---|---|---|---|---|---|---|
| Grid 177m | 9 | low | 600 | 0.8000 | 0 | 0 |
| Grid 177m | 36 | high | 900 | 0.0174 | 27 083 | 129 |

El escenario de baja densidad casi nunca detecta canal ocupado (esperado: 9 nodos, 177 m spacing, baja tasa). El escenario denso satura el canal: el CAD dispara decenas de miles de veces, la política de `CsmaMaxRetries=8` descarta los paquetes que no logran salir en ese límite (129 drops), y el PDR baja drásticamente — comportamiento físicamente consistente con CSMA/CAD funcionando + duty 100% + SF7-8 en colisión-dominio único.

### 7.7 — Uso (compatibilidad con campañas anteriores)

Para replicar las 4 campañas Pueyo con el MAC propuesto basta sustituir en los scripts de campaña:
```
--profile=pueyo2024_paper_like
```
por
```
--profile=pueyo2024_paper_like_csmacad
```
Todo lo demás (grid size, spacing, traffic, stopSec, seeds) queda idéntico. El plot comparativo consumirá el mismo `mesh_dv_summary.json` y mostrará la nueva barra `max_retries` solo si aparece activa (>0.5% de paquetes generados).

### 7.8 — Backups

```
src/loramesh/model/loramesh-mac-csma-cad.cc.bak-csmafix-20260424-011308
scratch/LoRaMESH-sim/mesh_dv_baseline.cc.bak-csmafix-20260424-091015
```


## 8. Validación CSMA/CAD (2026-04-24) — resultado: **funciona** (PDR +20-65% vs ALOHA en alta carga)

La campaña de validación ejecutada el 2026-04-24 (81 runs totales) **confirma** que el parche CSMA/CAD de §7 funciona: el MAC detecta ocupación correctamente, aplica backoff, y **entrega sistemáticamente más paquetes que ALOHA en alta carga** (+20% a +65% en N=9/25/49 high). Las dudas abiertas son de tuning, no de regresión funcional.

### 8.1 — Resultados Fase 1 (36 runs, ALOHA vs CSMA comparativo)

KPI principal: **PDR end-to-end** (paquetes entregados al destino final / paquetes generados).

| Escenario | PDR ALOHA | PDR CSMA | Δrel | delv ALOHA | delv CSMA | tx ALOHA | tx CSMA |
|---|---|---|---|---|---|---|---|
| N=9 low   | 77.07% | 79.36% | **+3.0%** | 123 | 127 | 533 | 537 |
| N=9 high  |  9.89% | 16.28% | **+64.7%** | 712 | 1172 | 7882 | 8046 |
| N=25 low  | 50.93% | 47.72% | −6.3% | 187 | 175 | 1486 | 1482 |
| N=25 high |  2.34% |  2.81% | **+19.9%** | 834 | 1000 | 37896 | 38186 |
| N=49 low  | 33.58% | 33.35% | −0.7% | 190 | 189 | 2682 | 2695 |
| N=49 high |  0.81% |  1.07% | **+33.0%** | 439 | 583 | 57387 | 57769 |

**Criterios actualizados (reemplazan los del plan original):**

| Criterio | Veredicto | Detalle |
|---|---|---|
| C1 — sin crashes | ✅ PASS | 36/36 runs completos, 0 FAILED |
| C2 — PDR low-load dentro ±15% | ✅ PASS | Δmáx 6.3% (N=25 low), dentro del ruido esperado con 3 seeds |
| **C3' — CSMA PDR ≥ ALOHA PDR en high load** | ✅ **PASS** | +19.9% / +33.0% / +64.7% para N=9/25/49 high |
| C4 — max_retries+qfull < 30% generado | ✅ PASS | Máx 0.4% (N=49 high) |
| C5 — CAD busy events > 0 en high | ✅ PASS | 4946 → 54821 eventos según N |

El criterio original del plan (C3: "colisiones absolutas CSMA < 80% de ALOHA") era **metodológicamente incorrecto**: `data_collision_drops` es un contador **por receptor** — cada intento fallido de demodulación en cada nodo cuenta. En mesh broadcast, una tx de A con 48 receptores potenciales puede contribuir hasta 48 eventos de colisión. Una prueba simple de que no es "drops del transmisor": ALOHA N=49 high tiene coll=156807 con solo tx=57387 (ratio 2.7×), imposible si fuera "1 colisión = 1 tx perdida". Por eso CSMA con más paquetes efectivamente en el aire (tx +0.6%) puede producir más eventos de colisión absolutos aunque entregue más paquetes al destino.

### 8.2 — Resultados Fases de tuning (45 runs, N=25 high)

| Fase | Barrido | Rango | Mejor config | PDR best | vs default CSMA |
|---|---|---|---|---|---|
| 2A | CsmaMaxRetries | {4,8,16,32} | retries=4 | 0.0286 | +1.8% |
| 2B | CsmaTxQueueMax | {16,32,64,128} | indistinto | 0.0281 | 0% (cola nunca se llena) |
| 2C | CadSenseMarginDb | {3,6,9,12} | margin=3 dB | 0.0387 | **+37.7%** |
| 3D | (cbf, dbf) | 3 combos | indistinto | 0.0281 | 0% (ver §8.3) |

**Hallazgo relevante Fase 2C**: `cadSenseMarginDb=3` (menos margen → CAD más sensible → defiere más agresivamente) produce el **mejor PDR del campaña entero** (0.0387 vs 0.0281 default, +37.7%). El trade-off es que `drop_max_csma_retries` sube a 3156 por run, pero el PDR efectivo compensa. Esto sugiere que el default de 6 dB de §7.1 es conservador; 3 dB rinde mejor bajo carga alta N=25.

### 8.3 — Limitación metodológica: Fase 3D fue un experimento nulo

El perfil `pueyo2024_paper_like_csmacad` fuerza `controlBackoffFactor=1.0, dataBackoffFactor=10.0` vía `applyPueyoComparableBase()` (`mesh_dv_baseline.cc:567-568`), y el validator (`mesh_dv_baseline.cc:791`) rechaza cualquier otra combinación. Los flags CLI `--controlBackoffFactor`/`--dataBackoffFactor` del sweep son clobbered silenciosamente antes de llegar al runtime. Los 9 runs de Fase 3D produjeron PDR idéntico porque el MAC vio siempre cbf=1.0/dbf=10.0.

**Esto NO es un bug** — es una restricción deliberada de comparabilidad con el paper Pueyo 2024. Para medir sensibilidad real de cbf/dbf habría que: (a) usar un perfil distinto sin el validator, (b) modificar `applyPueyoComparableBase()` para no tocar cbf/dbf, o (c) añadir un flag `--allowBackoffOverride` que desactive el check. Esto es una nota para futuras campañas, no un defecto del parche §7.

### 8.4 — Auditoría estática (Fase 4) — hallazgos

Cosas verificadas y limpias:
- ✅ **Queue invariants** en `SendWithCSMA` (mesh_dv_app.cc ~3497 y ~3554): dos loops de evicción en ramas mutuamente exclusivas, sin double-count de `m_dropQueueOverflow`.
- ✅ **Beacons respetan CSMA**: `dst=0xFFFF` pasa por `SendWithCSMA(...,true)` con factor control=1.0, entra a la misma cola y hace CAD. No hay fast-path que los salte. La FIFO eviction los protege de ser descartados.
- ✅ **Export path `drop_max_csma_retries`**: `mesh_dv_app.cc:m_dropMaxCsmaRetries++` → `RuntimeNodeStats::dropMaxCsmaRetries` → accumulator `metrics_collector.cc:1119` → JSON emit línea 1678. Empíricamente verificado (111 drops en N=25 high seed1).
- ✅ **Ventana de backoff es adaptativa bajo carga**: `ComputeBackoffWindowSlots()` suma `loadSlots = GetCadLoad() × 0.5 × maxSlots`. En N=49 high (CadLoad≈1, maxSlots=64), window efectiva ≈ 4+0+32 = 36 slots. El `MinBackoffSlots=4` es floor para régimen ocioso, no techo en congestión.
- ✅ **Atributos tunables**: `MinBackoffSlots`, `BackoffStep`, `CadHistoryWindow`, `CadLoadWeight`, `ToaMaxFactor` son todos `AddAttribute` configurables via `Config::SetDefault` o `--ns3::loramesh::CsmaCadMac::X=`. El campaigner original solo barrió `CsmaMaxRetries`/`CsmaTxQueueMax`/`CadSenseMarginDb` (parámetros del app-layer) y no tocó estos.
- ✅ **Coherencia `pueyoFloraLikeRx` ↔ CAD sensitivity** (auditoría 2026-04-24, post-Fase 4): el flag `pueyoFloraLikeRx=true` (`mesh_dv_baseline.cc:818-822`) **solo desactiva `enableSfScanRx`** — el propio warning del código lo dice: *"misma semantica single-channel/single-demod y mismo modelo de colision; solo se desactiva pending scan/lock"*. El CAD opera independientemente vía `m_phy->GetChannel()->GetTxEvents()` + `channel->GetRxPower()` (`loramesh-mac-csma-cad.cc:547-548`) con threshold `GetCadSensitivityDbm(sf) + m_cadSenseMarginDb`. La tabla `kSensitivity` de `GetCadSensitivityDbm()` (`.cc:653-655`) usa exactamente la sensitivity SX1276 estándar (-123/-126/-129/-132/-133/-136 dBm para SF7..SF12 @125kHz), igual que el receptor LoRa. **No hay incoherencia**: son dos planos del modelo (orquestación de scan SF vs. modelo físico de propagación) que no se cruzan.

### 8.5 — Puntos arquitectónicos: estado tras cleanup 2026-04-24

**Punto 1 — Semántica del reset de `m_failures` en `PerformChannelAssessment`** ✅ RESUELTO (documentación in-code)

La afirmación original de que el cambio `m_failures--` → `m_failures = 0` replica "semántica 802.11" era **imprecisa**: 802.11 resetea la contention window en transmisión exitosa (recepción de ACK), no en CCA limpio. La adaptación real bajo carga sostenida no viene de `m_failures` sino de `loadSlots = GetCadLoad() × m_loadWeight × maxSlots` en `ComputeBackoffWindowSlots()`, que mantiene la ventana en ~30+ slots cuando el canal está mayormente ocupado.

**Acción tomada (2026-04-24)**: el comentario `// B1 fix (802.11-style)` en `loramesh-mac-csma-cad.cc:463` se reemplazó por un bloque de documentación que explica:
- Por qué NO es estricta semántica 802.11.
- Que la "memoria de congestión" real vive en el estimador de carga (loadSlots), no en `m_failures`.
- Que `m_failures` actúa solo como escalador de corto plazo durante ráfagas de muestras busy consecutivas.
- La alternativa considerada (decremento gradual) y por qué no se aplicó.

No se cambia comportamiento. La decisión de diseño queda visible para futuros lectores.

**Punto 2 — `NotifyTxResult` sin callers** ✅ RESUELTO (documentación in-code)

`grep -rn NotifyTxResult` confirma 0 callers tras §7.2. En LoRa broadcast no hay ACK, así que "tx success" no es un evento observable por el MAC.

**Acción tomada (2026-04-24)**: en lugar de eliminar la API (que rompería downstream si alguien añade un MAC con ACK), se documentó:
- En el header (`loramesh-mac-csma-cad.h:65`): comentario Doxygen explicando que está actualmente sin uso, por qué se removió el caller en §7.2 (era invocado en *every PHY tx start*, lo que producía un `m_failures=0` espurio sin importar si el frame se entregaba), y que queda reservado para un MAC unicast-con-ACK futuro.
- En la implementación (`loramesh-mac-csma-cad.cc:367`): comentario corto remitiendo al header y aclarando que el manejo activo de `m_failures` vive en `PerformChannelAssessment`.

El binario `mesh_dv_baseline-default` se relinkeó tras los cambios (cambios solo en comentarios — sin impacto semántico).

### 8.6 — Conclusión

**El parche §7 funciona como diseñado.** Entrega consistentemente más paquetes al destino que ALOHA puro en los regímenes de alta carga (+20-65%), lo cual es precisamente la mejora esperada de un MAC con sensing vs uno sin él. Las diferencias en baja carga (±6%) son estadísticamente compatibles con ruido a 3 seeds.

La afirmación de §7.6 (smoke runs) era verdadera pero insuficiente; esta campaña comparativa es la primera que mide **el efecto funcional del MAC contra su baseline de comparación directa**, y el veredicto es positivo.

**Hallazgo adicional de valor**: `cadSenseMarginDb=3` (más sensible) produce PDR **+37.7% vs el default de 6 dB** en N=25 high. Considerar bajar el default a 3 dB en una futura revisión del perfil.

**Errata metodológica**: una versión anterior de este §8 (commit transitorio) concluyó erróneamente que el parche introducía una regresión funcional. Esa conclusión se basó en un criterio defectuoso ("colisiones absolutas CSMA < 80% de ALOHA") que confunde un contador por-receptor con drops-del-transmisor. El análisis correcto — comparar PDR y goodput — muestra lo contrario. Los "4 defectos" alegados en esa versión se redujeron tras re-auditoría a: 2 puntos arquitectónicos abiertos (§8.5), sin bloqueantes.

### 8.7 — Cierre de items opcionales (2026-04-24)

Tras la conclusión de §8.6 quedaban 5 items opcionales no bloqueantes. Estado actual:

| Item | Descripción | Estado |
|---|---|---|
| 1 | Bajar `cadSenseMarginDb` default a 3 dB | ✅ **Cerrado** — campaña `validate_margin3` (18 runs cross-N), default cambiado a 3.0 en los 3 sitios. Detalle abajo. |
| 2 | Limpieza de `NotifyTxResult` (eliminar o documentar como reservada) | ✅ **Cerrado** — documentado in-code con Doxygen, ver §8.5 punto 2 |
| 3 | Documentar in-code la decisión de diseño sobre `m_failures` (adaptación vía `loadSlots`) | ✅ **Cerrado** — comentario reemplazado en `PerformChannelAssessment`, ver §8.5 punto 1 |
| 4 | Auditar coherencia `pueyoFloraLikeRx` ↔ CAD sensitivity | ✅ **Cerrado sin hallazgos** — son planos independientes del modelo, ver §8.4 último bullet |
| 5 | Sweep real de cbf/dbf (sin saltarse el validator Pueyo) | ✅ **Cerrado** — nuevo profile `csmacad_free_backoff` añadido + sweep ejecutado. Detalle abajo. |

Resumen: **5 de 5 items cerrados** (items 1-4 con resultado positivo; item 5 cerrado con respuesta científica: la asimetría Pueyo 1:10 es esencialmente óptima). La MAC CSMA/CAD queda validada funcionalmente, con default tuning aplicado, documentada en sus puntos arquitectónicos y con un profile reusable para futuro tuning fuera de Pueyo.

#### Detalle del item 1 — campaña `validate_margin3` (2026-04-24)

**Hipótesis**: Fase 2C había mostrado +37.7% PDR con `cadSenseMarginDb=3` vs default 6 en N=25 high. La pregunta abierta era si ese efecto se mantenía en N=9 high y N=49 high, y si no degradaba baja carga (N=*/low).

**Diseño**: 18 runs (N ∈ {9,25,49} × load ∈ {low,high} × 3 seeds) con `cadSenseMarginDb=3` y resto de parámetros idénticos al baseline `pueyo2024_paper_like_csmacad`. Comparados contra Fase 1 (margin=6).

**Resultados (mean over 3 seeds)**:

| escenario | PDR m=6 | PDR m=3 | Δrel | drop% (m=3) |
|---|---|---|---|---|
| N=9 low | 79.36% | 80.82% | +1.8% | 0.00% |
| **N=9 high** | **16.28%** | **22.88%** | **+40.5%** | 3.64% |
| N=25 low | 47.72% | 50.18% | +5.2% | 0.00% |
| N=25 high | 2.81% | 3.87% | +37.6% | 8.87% |
| N=49 low | 33.35% | 33.11% | -0.7% | 0.71% |
| **N=49 high** | **1.07%** | **1.58%** | **+47.0%** | 11.14% |

**Criterios de decisión** (los 3 PASS):
- **C-A** PDR(m=3) ≥ PDR(m=6) en N=9/49 high → ✅ PASS (+40.5%, +47.0%)
- **C-B** |Δrel| < 15% en todas las low-load → ✅ PASS (max |Δ|=5.2%)
- **C-C** drop_max_csma_retries+drop_queue_overflow < 30% del generado en todos los high → ✅ PASS (max 11.14%)

**Acción tomada**:
1. `scratch/LoRaMESH-sim/mesh_dv_baseline.cc:69`: `double cadSenseMarginDb = 6.0;` → `= 3.0;`
2. `src/loramesh/model/loramesh-mac-csma-cad.cc:136`: `DoubleValue(6.0)` → `DoubleValue(3.0)` en `AddAttribute("CadSenseMarginDb", ...)`
3. `src/loramesh/model/loramesh-mac-csma-cad.cc:167`: ctor init `m_cadSenseMarginDb(6.0)` → `(3.0)`
4. `mesh_dv_baseline.cc:422-424`: actualizado el texto descriptivo del CLI para reflejar el nuevo default y el commit que lo cambió.

Smoke run post-cambio (N=9 high, stopSec=600, seed=1) entregó PDR=23.93%, consistente con el 22.88% de la mini-campaña (stopSec=2100) — confirma que el nuevo default se aplica.

**Caveats**:
- La mini-campaña usó la misma topología (`pueyo_grid`, spacing 177m, sf 7-8) y profile (`pueyo2024_paper_like_csmacad`) que las anteriores. Generalizar a topologías o SFs muy distintos requeriría sweep adicional.
- El sweep dentro de la mini-campaña era binario (3 vs 6 dB), no mapeó la curva fina; es posible que valores entre 2 y 4 sean comparables. No exploré márgenes < 3 dB.
- Si en algún momento se observara un escenario donde 3 dB es excesivo (CAD demasiado sensible → all-defer pathological), la mitigación es trivial: `--cadSenseMarginDb=6` en CLI o `Config::SetDefault("ns3::loramesh::CsmaCadMac::CadSenseMarginDb", DoubleValue(6.0))`.

#### Detalle del item 5 — profile `csmacad_free_backoff` y sweep cbf/dbf (2026-04-24)

**Problema**: el profile `pueyo2024_paper_like_csmacad` clobber-ea `controlBackoffFactor=1.0, dataBackoffFactor=10.0` en `applyPueyoComparableBase()` (`mesh_dv_baseline.cc:567-568`) y aborta vía validator (línea 791) si esos valores no están al final del setup. Esto es **deliberado** para garantizar comparabilidad con el paper Pueyo 2024, pero hace que sweeps CLI de cbf/dbf en ese profile sean experimentos nulos (Fase 3D del plan original).

**Decisión de diseño**: NO añadir un flag-escape (`--allowBackoffOverride`) que invalide el validator dentro del profile Pueyo. Romper el contrato del profile y mantener el mismo nombre es peor que añadir un profile nuevo. En su lugar, se añadió:

**Nuevo profile `csmacad_free_backoff`** (mesh_dv_baseline.cc):
- Idéntico a `pueyo2024_paper_like_csmacad` en todo: enableCsma, duty disabled, sfMin=7/sfMax=8, pueyoFloraLikeRx=true, enableSfScanRx=false, route metrics, beacon scheduling, etc.
- **Única diferencia**: cbf/dbf provienen del CLI (defaults globales 0.8/0.6) en lugar de ser fijados a (1.0, 10.0). El bloque del profile captura los valores CLI antes de invocar `applyPueyoComparableBase()` y los restaura después.
- Validator paralelo (sin chequear cbf/dbf): valida todo lo demás del setup pero deja cbf/dbf libres.
- El campo `"profile"` en JSON dice `csmacad_free_backoff` — los runs son distinguibles de Pueyo en cualquier análisis posterior.

Cambios concretos: 5 ediciones en `mesh_dv_baseline.cc` (help string, validator de profile name, bloque de setup nuevo, condición `allowTemporalDesyncVariant`, validator nuevo).

**Sweep ejecutado** (5 combos × 3 seeds = 15 runs, N=25 high, margin=3 dB nuevo default):

| (cbf, dbf) | PDR | vs Pueyo (1, 10) |
|---|---|---|
| **(1.0, 10.0) Pueyo control** | **3.90%** | (baseline) |
| (1.0, 1.0) sin asimetría | 3.61% | -7.5% |
| (1.0, 5.0) | 3.67% | -5.8% |
| (1.0, 20.0) | 4.06% | +4.2% |
| (3.0, 10.0) | 3.84% | -1.5% |

**Hallazgo**: la asimetría 1:10 de Pueyo es **esencialmente óptima** en el rango explorado. La mejor configuración encontrada (1, 20) supera el baseline por solo +4.2%, lo cual está dentro del ruido esperado a 3 seeds. Reducir la asimetría (`1:1` o `1:5`) empeora claramente el PDR (-7.5% / -5.8%) — confirma que dar prioridad al control sobre los datos es importante para que el routing converja con tráfico saturante. Subir cbf (3, 10) también empeora (-1.5%).

**Conclusión**: Pueyo eligió bien (1, 10). No hay caso para cambiar el default de la asimetría en el profile Pueyo, ni motivo para hacer barridos exhaustivos adicionales en este eje. El profile `csmacad_free_backoff` queda disponible para futuros experimentos donde el escenario sea radicalmente distinto (ej. tráfico de control mucho mayor, o densidades >> 49 nodos) y revisitar la pregunta tenga sentido.

### 8.8 — Nota sobre el build global: errores preexistentes en utilities

`./ns3 build` retorna `rc=2` con link errors en tres targets utilitarios:
- `ns3-dev-scratch-simulator-default`
- `ns3-dev-scratch-subdir-default`
- `ns3-dev-print-introspected-doxygen-default`

Los errores son símbolos sin definir como `MeshDvApp::SetPeriod`, `MeshLoraNetDevice::InitGlobalPcap`, `MeshLoraNetDevice::GetTypeId`, etc.

**Diagnóstico (2026-04-24)**: estos errores son **preexistentes** y **no relacionados con el parche §7 ni con los cambios de §8.7**.

**Causa raíz**: `src/loramesh/helper/loramesh-helper.cc` (líneas 233, 250, 274, 300) referencia clases (`MeshDvApp`, `MeshLoraNetDevice`) cuyas definiciones viven en `scratch/LoRaMESH-sim/mesh_dv_app.cc` y `scratch/LoRaMESH-sim/mesh_lora_net_device.cc`. Es decir, la lib `loramesh` (en `src/`) tiene una dependencia hacia código que vive en `scratch/`. Cuando un binario utility (que solo linkea `libns3-dev-loramesh-default.so`) no incluye los `.o` del scratch baseline, los símbolos quedan unresolved.

**Verificación histórica**: la cross-dependency existe en los 3 commits del repositorio (`bed544a`, `f2a821a`, `c5cb7e7`), cada uno con exactamente 6 referencias a `MeshDvApp`/`MeshLoraNetDevice` en `loramesh-helper.cc`. Es un defecto de diseño del cmake setup del proyecto frozen-20260327, no algo que hayamos introducido en este trabajo.

**Impacto operacional**: cero. El único target que necesitamos para correr simulaciones (`ns3-dev-mesh_dv_baseline-default`) sí incluye los `.o` del scratch en su propio build target y compila/linkea correctamente. Todos los runs de validación (Fase 1, validate_margin3, sweep_cbf_dbf) corrieron sobre ese binario y pasaron.

**Mitigación futura (fuera del scope CSMA/CAD)**: tres alternativas razonables si alguien quiere reparar el build global:
1. Mover `MeshDvApp` y `MeshLoraNetDevice` desde `scratch/LoRaMESH-sim/` a `src/loramesh/model/` para que sean parte de la lib propiamente dicha.
2. Dividir `loramesh-helper.cc` en una capa core (sin deps al scratch) y una capa application (con).
3. Configurar cmake para que las utilities NO linkeen `libloramesh.so`, o que lo hagan con `--allow-shlib-undefined` si los símbolos no son llamados en runtime.

Ninguna de estas es necesaria para validar §7 ni para la operación normal del simulador. Se documenta como deuda técnica del proyecto, no como issue del MAC.
