# VALIDATION.md — registro vivo de V&V del simulador DV-CL

Bitácora de verificación y validación (Plan Maestro, principio rector).
Cada entrada: qué se verificó, contra qué verdad, resultado, y dónde está
el test automatizado. Este archivo alimenta la subsección "Simulator
Validation" del paper y la respuesta preempaquetada a revisores.

---

## 2026-07-17 — F1.1 Time-on-Air == Semtech AN1200.13 (PASS)

- **Qué:** `ComputeLoRaToAUs` del simulador (transpilada fielmente)
  contra una implementación independiente de la fórmula de Semtech
  AN1200.13 / SX1276 §4.1.1.6.
- **Cobertura:** grid completo SF7-12 × BW{125,250} × CR{4/5..4/8} ×
  payload{1..255 muestreado} × DE{on,off} × CRC{on,off} × IH{on,off} =
  **7296 combos, 0 mismatches** (igualdad exacta al µs).
- **Anclas públicas:** SF7/BW125/CR4-5/20B = 56.576 ms ✔;
  SF12/BW125/CR4-5/51B/DE = 2465.792 ms ✔ (calculadora TTN).
- **Test:** `contrib/dv-cl/test/reference/toa_reference.py` (genera
  además `toa_golden.csv`); espejo C++ en el TestSuite `dv-cl`
  (`./test.py -s dv-cl`).
- **Nota:** el matiz de dónde se aplica `max(...,0)` (sim: sobre el
  cociente antes de `ceil`; Semtech: sobre el término completo) es
  algebraicamente equivalente — ambos colapsan a 0 exactamente cuando el
  numerador ≤ 0. Confirmado numéricamente en todo el grid.

## 2026-07-17 — Métrica compuesta: valores analíticos y monotonicidad (PASS, unit)

- **Qué:** `DvClCompositeMetric` (fórmula de la tesis: ΔC = α·ToA_hat +
  β + δ·Ψ(b); defaults 0.60/0.15/0.25; Ψ por tramos bLo=0.20, bHi=0.50,
  p=2, ΨMax=1) contra valores calculados a mano y chequeos de
  monotonicidad (más ToA ⇒ más costo; menos SoC ⇒ más costo), bordes
  (b=bHi, b=bLo, b<bLo, b desconocido) y saturación de la normalización.
- **Test:** `contrib/dv-cl/test/dv-cl-test-suite.cc`
  (`DvClMetricAnalyticTestCase`).

## 2026-07-17 — Módulo contrib/dv-cl compila y su suite pasa en ns-3.46 (PASS)

- **Qué:** `contrib/dv-cl` colocado en un árbol ns-3.46.1-dev (WSL
  Ubuntu, gcc 13.3), configurado con
  `./ns3 configure --enable-tests --enable-examples --enable-modules=dv-cl`.
- **Resultado:** compila sin warnings propios;
  `test-runner --suite=dv-cl` → **PASS** (2/2 TestCases: golden ToA +
  métrica analítica/monotonicidad); el ejemplo `dv-cl-toa-example`
  corre e imprime el barrido de batería reproduciendo la Ψ por tramos
  (plano ≥bHi, rampa cuadrática, saturación ≤bLo).
- Es el smoke del Gate 5: estructura de módulo estándar verificada
  contra instalación limpia.

## 2026-07-18 — F0.4(a) Determinismo misma-máquina (PASS)

- **Qué:** mismo escenario (a2a N=25 grid, cmp_csma, dcoff) + misma
  semilla (`rngRun=7`) ejecutado dos veces en el servidor de campañas.
- **Resultado:** `mesh_dv_summary.json` **bit-idéntico** y las 5 tablas
  de métricas (delay/duty/energy/lifetime/tx) idénticas byte a byte.
- **Test:** `/home/diego/sim/_f04_determinism.sh` (servidor).
- Pendiente F0.4(b): determinismo cross-máquina (requiere el módulo
  portable) y golden traces de regresión (abajo).

## 2026-07-18 — F0.3 metadatos por corrida + F0.4(b) golden traces (OPERATIVOS)

- **F0.3:** wrapper `/home/diego/sim/run_with_metadata.sh` emite
  `run_metadata.json` por corrida: commit hash del árbol, nº de archivos
  sucios, SHA256 y ruta del binario, argv completo, timestamps y
  duración, hostname. Verificado sobre el snapshot
  `server-state-20260718` (commit 92bb756, árbol limpio).
- **F0.4(b):** golden traces de 3 escenarios chicos con el binario 6B
  actual, en `/home/diego/sim/golden_traces/` con manifiesto
  `GOLDEN_SHA256`: g1 a2a N9 cmp_csma dcoff · g2 conv N25 toa_aloha dc1
  · g3 a2a N9 cmp_csma dc1. Regla: cualquier cambio de código se
  compara contra estos hashes; si el cambio es intencional, el golden se
  actualiza explícitamente en el commit.

## 2026-07-18 — Dato 6B vs 7B a escala completa (PASS)

- **Qué:** re-run 6B completo (18 campañas, terminado 2026-07-05)
  contra el dato 7B canónico, todas las celdas del spine (3 modelos ×
  4 variantes × 2 topologías × 2 regímenes DC × N).
- **Resultado:** celdas headline prácticamente idénticas (p.ej. a2a
  grid dcoff N49: toa 14.50→14.60, cmp 16.79→16.70); máx |ΔPDR| =
  2.26 pp (celda N=9 de alta varianza); **edge energético del SoC
  preservado** (minrem cmp−toa en a2a: +0.049 → +0.053). El dataset 6B
  queda validado como equivalente con 1 byte menos de beacon.

## 2026-07-18 — F1.4(a) Invariante de duty cycle, corpus 6B completo (PASS)

- **Qué:** `dutyUsed` por nodo en TODOS los runs dc1 del corpus 6B
  (spine a2a/conv/msink + soc_ablation + wsweep).
- **Resultado:** **55 600 nodos-run bajo EU868 1 %: cero violaciones**;
  el máximo observado es exactamente 0.01000 (el enforcement satura en
  el límite). Contraste sin límite (dcoff): máx 0.0886 — el limitador
  está genuinamente apagado ahí. Verificación corpus-completa del claim
  regulatorio central del paper.
- **Pendiente (instrumentación):** versión estricta por ventana
  deslizante requiere columna ToA por transmisión en el log de tx; se
  añade en el port del módulo.
- **Test:** `/home/diego/sim/_f14_f13_sweep.py`.

## 2026-07-18 — F1.3(a) Cierre contable de energía, corpus 6B (PASS con nota)

- **Qué:** invariante inicial − consumida = restante por nodo, 111 200
  nodos-run.
- **Resultado:** error máximo 0.010 J sobre baterías de ~3888 J
  (2.6 ppm) — exactamente el redondeo del CSV, que loguea con 2
  decimales; el cross-check `energyFrac` vs restante/inicial (precisión
  completa) da error máx 1.8e-04. **El cierre se cumple dentro de la
  precisión del log.** Nota para el port: subir la precisión del CSV de
  energía (≥6 decimales) para poder apretar el umbral del assert.
- Pendiente F1.3(b): E_tx = V·I·ToA al mJ contra datasheet (necesita la
  columna ToA por tx) y T50 analítico en escenario trivial.

## 2026-07-18 — Primer ciclo de cambio controlado: columna toaUs (CERTIFICADO)

- **Cambio:** columna `toaUs` por transmisión en `mesh_dv_metrics_tx.csv`
  (struct → RecordTx → LogTxEvent → call-site → 2 escritores). Commit
  `790d587` del árbol override.
- **Certificación logging-only vía golden traces:** con el binario
  nuevo, los 3 `mesh_dv_summary.json` son **bit-idénticos** a los
  goldens v1, y los `tx.csv` difieren **exclusivamente** en la columna
  nueva (verificado con `cut` de la col. 12 + diff). Goldens promovidos
  a v2 (v1 archivado en `golden_traces_v1_pre_toacol/`),
  `GOLDEN_SHA256` regenerado — actualización explícita según F0.4.

## 2026-07-18 — F1.4(b) Duty cycle en ventana deslizante (HALLAZGO de semántica)

- **Qué:** con la columna ToA real, máximo airtime por nodo en ventana
  rodante de 1 h sobre los goldens dc1.
- **Resultado:** conv 1.21 %, a2a 1.12 % (dcoff de referencia: 5.5 %) —
  supera el 1 % en ventana rodante, mientras el agregado clava
  ≤ 1.000 % exacto (55 600 nodos-run, F1.4a).
- **Mecanismo (confirmado en código):** el MAC aplica presupuesto por
  **ventana fija de 1 h** (`SetDutyCycleWindow(Hours(1))`,
  `GetDutyCycleUsed/Limit`). Una ráfaga que cruza el borde de ventana
  puede alcanzar hasta 2× teórico en ventana rodante; observado ≤ 1.21×.
- **Disposición:** implementación literal defendible de ETSI EN 300 220
  (duty definido por hora); la lectura rodante es más conservadora. Se
  declara explícitamente en el paper ("presupuesto por ventana fija de
  1 h"). Para el port del módulo: semántica configurable en
  `RegionalProfile` (ventana fija | rodante | T_off por transmisión
  estilo Semtech), con la rodante como default estricto.

## Hallazgos de auditoría (F0.2)

1. **[RESUELTO 2026-07-18 — benigno] Dualidad de métricas.** El frozen
   (2026-03-27) implementa `CompositeMetric` con α=0.40/β=0.30/δ=0.30 y
   Ψ asintótica (generación vieja). En el **override** (binario de las
   campañas del paper) `CompositeMetric` **fue actualizado** a la
   fórmula de la tesis — α=0.60/β=0.15/δ=0.25 con Ψ por tramos
   (b_w=0.50, b_c=0.20, p=2) — de modo que las DOS copias
   (`CompositeMetric` usada por la app y `ComputeThesisLinkCost` usada
   por routing-dv) computan la MISMA fórmula. Los datos de campaña no
   tienen inconsistencia. Queda como deuda de mantenibilidad (dos copias
   de la misma fórmula = riesgo de divergencia futura); el port a
   `contrib/dv-cl` las unifica en `DvClCompositeMetric` tras la
   interfaz `DvClRoutingMetric`.
2. **Procedencia de `kMaxToaUs`.** La tabla de normalización
   {143360, 256512, 462848, 829440, 1810432, 3293184} µs declara
   "max payload 222B" en su comentario, pero AN1200.13 a 222B da otros
   valores (p.ej. SF12/222B/DE = 8 036 352 µs). Como constante interna de
   normalización no afecta el ranking (solo la escala de ToA_hat), pero
   el comentario es incorrecto. Mantener los valores por paridad de
   comportamiento con las campañas; corregir la documentación.
3. **Integración loramesh rota en el árbol WSL ns-3-dev (preexistente).**
   El `~/ns3/ns-3-dev` de WSL ya traía `src/loramesh` + `scratch/LoRaMESH-sim`
   integrados, con dependencia invertida: `libns3-dev-loramesh.so` referencia
   símbolos definidos en scratch (`ns3::MeshDvApp`, `ns3::lorawan::MeshLoraNetDevice`)
   → el link de ejecutables falla. Anti-patrón que el port a `contrib/dv-cl`
   elimina (la librería no puede depender de scratch). No bloquea a dv-cl
   (validado aislado con `--enable-modules=dv-cl`).
4. **Episodio del wire del beacon (2026-06/07).** El header pueyo7b
   nunca viajaba al aire: el rebuild de TX re-emitía un header V2
   gemelo byte-a-byte (7B). Reducirlo a 5B corrió las entradas DV 2
   bytes (PDR colapsado en todas las variantes); el fix posterior
   olvidó copiar el SoC (término de energía inerte, detectado por la
   δ-ablación d0==d25). Resuelto y validado en el binario 6B (5 base +
   1 SoC; δ-ablación restaurada). **Lección → el plan:** exactamente la
   clase de bug que los golden traces de F0.4 y el test de simetría de
   F1.6 detectan; el port a módulo unifica los dos headers y elimina el
   rebuild.

## Backlog de verificación (Gates 0-2 del plan)

- F0.4 determinismo bit-idéntico + golden traces de 3-4 escenarios.
- F1.3 contabilidad de energía (E = V·I·ToA al mJ; balance total).
- F1.4 invariante de duty cycle por ventana deslizante (assert siempre).
- F1.5 convergencia vs Bellman-Ford offline (NetworkX).
- F2.1 equivalencia single-hop vs módulo lorawan estándar.
- F2.3 ancla ALOHA S=G·e^(−2G) y bound n·δ·(payload/ToA).
- F2.4 conservación de paquetes (generados = entregados + desglose).
