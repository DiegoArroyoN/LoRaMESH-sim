# Fixes de validez B2, B4, B5 — 2026-04-17

Autor: auditoría de validez solicitada por Diego (post alineación SX1272, antes de campañas).
Objetivo: corregir tres problemas de código/semántica que **no** afectan la fidelidad al paper Pueyo 2024 pero sí comprometían la correctitud o reproducibilidad del simulador.

Backups: `*.bak_B245_20260417_190706` en cada ruta.

## B2 — Ventana de deduplicación

**Problema.** `m_dedupWindow` = 600s por defecto. En saturación o runs largos, entradas de `m_deliveredSet` (por dst) y `m_seenOnce` (anti-loop por relay) se purgaban después de 10 min. Si llegaba un duplicado tardío:
* un relay lo volvía a reenviar (su `m_seenOnce` ya purgada)
* el destino lo contaba como nueva entrega (su `m_deliveredSet` ya purgada)

Resultado: **doble-conteo** de entregas → PDR inflado artificialmente.

**Fix.**
* `scratch/LoRaMESH-sim/mesh_dv_app.h:472`: default pasó a `Seconds(86400)` (24h).
* `scratch/LoRaMESH-sim/mesh_dv_baseline.cc:130`: default CLI pasó a `86400.0`.

Con 24h ninguna run paper_like (tipicamente 2100–10800s) purga. El CLI `--dedupWindowSec` sigue disponible para runs experimentales.

**Impacto memoria.** Para N=64, 100 pkts/par, ~6300 entries por nodo destino y ~400K entries por relay en worst case (~12 MB). Aceptable.

**Validación smoke (N=9 grid 177m 2100s):**
* Pre-fix (dedupWindow=600s): PDR ≈ 77 % (con doble-conteo)
* Post-fix (dedupWindow=86400s):
  * rngRun=1: TX_orig=1755, RX_final=938, PDR=53.4 %
  * rngRun=2: TX_orig=1757, RX_final=1307, PDR=74.4 %
  * rngRun=7: TX_orig=1778, RX_final=866, PDR=48.7 %

La caída del número previo es la *corrección* del sesgo, no una regresión.

## B4 — AssignStreams por subsistema

**Problema.** Todas las llamadas `CreateObject<UniformRandomVariable>()` usaban stream=0 implícito. Al variar N o rngRun, los flujos de números se acoplaban entre subsistemas y entre nodos → reproducibilidad frágil.

**Fix.** Asignación determinista de `SetStream(int64_t)` por subsistema:

| Archivo | Sitio | Rango stream |
|---|---|---|
| `mesh_dv_app.cc:974` | lazy init `m_rng` | `1_000_000 + nodeId` |
| `mesh_dv_app.cc:1471` | `StartApplication` `m_rng` | `1_000_000 + nodeId` |
| `mesh_dv_baseline.cc:1335` | `socRng` (SOC inicial) | `10` (singleton) |
| `loramesh-routing-dv.cc:SetNodeId` | `m_rng` routing | `2_000_000 + nodeId` |

* MAC CSMA/CAD hereda el RNG del app vía `SetRandomStream`, así que queda dentro del rango `1_000_000 + nodeId`.
* Rangos de 1M dejan `65535` nodos sin colisión; bien lejos del rango auto-asignado de ns-3.

**Validación reproducibilidad.** 3 runs con `rngRun=1` → md5 idéntico (`84c7b7a72a910c415d7781792c9fa3c4` para rx, `2b8f7c8171ce490a9a9f7c5b49ee7e6c` para tx). `rngRun=2` produce CSVs distintos, confirmando que el RNG está viva pero controlada.

## B5 — Fix contabilidad de entregas en path v1

**Problema.** `MeshDvApp::ForwardWithTtl` (path legacy v1/v2, `mesh_dv_app.cc:2285`):

```cpp
if (myId == dst) {
    if (myId == m_collectorNodeId) {     // <-- sólo contaba si yo era el sink
        m_dataPacketsDelivered++;
        ...
    }
    return;
}
```

En `pueyo_all_to_all` con wireFormat legacy (`v1`/`v2`) y **múltiples destinos**, entregas a nodos que no son el collector no se contaban. PDR reportado falsamente bajo.

**Por qué no afecta paper_like hoy.** `validatePueyoComparableBase` (línea 628) ya fuerza `wireFormat=pueyo7b`. El path pueyo7b tiene su propio conteo correcto en `mesh_dv_app.cc:4660+`. El bug sólo se manifiesta si alguien usa un perfil no-Pueyo con `pueyo_all_to_all` y wireFormat v1/v2.

**Fix.** Se elimina el gate `if (myId == m_collectorNodeId)` manteniendo un scope anónimo `{ ... }` para no cambiar la indentación del bloque original. Ahora toda entrega a `myId == dst` incrementa `m_dataPacketsDelivered`, alineado con el path pueyo7b.

**Implicaciones futuras.**
* `periodic_any_to_any` con un único sink sigue funcionando igual.
* `pueyo_all_to_all` con wireFormat no-Pueyo ahora cuenta entregas correctamente (útil si alguna vez se quiere comparar v1 vs pueyo7b en condiciones idénticas).
* Traffic mode `any_to_one` / `all_to_one` explícito aún **no existe** (solo hay `periodic_any_to_any` y `pueyo_all_to_all`); se agregará como modo futuro si se requiere.

## Build / smoke

```
./ns3 build mesh_dv_baseline    # [100%] compila sin warnings
LD_LIBRARY_PATH=build/lib ./build/scratch/LoRaMESH-sim/ns3-dev-mesh_dv_baseline-default     --profile=pueyo2024_paper_like --nEd=9 --nodePlacementMode=pueyo_grid     --pueyoGridSpacingM=177 --pueyoGridSide=3 --stopSec=2100 --rngRun=1
# exit=0, CSVs exportados, DV tables llenas, reproducibilidad OK
```

## Pendientes asociados

* **B1** (aún sin aplicar): contador separado `m_originPendingAtStop` para no deflactar PDR al cortar simulación mientras quedan paquetes en cola de origen.
* **B3** (config, no código): recomendar `dataStartSec` que escale con N y beacon interval para asegurar convergencia DV antes de arrancar data.
* **B6**: resuelto automáticamente por B4 (el tie-break RNG es el mismo `m_rng` routing con stream determinista).

