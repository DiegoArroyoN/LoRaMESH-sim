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
- **Mecanismo (REFINADO 2026-07-19 al portar el MAC):** la clase
  `CsmaCadMac` implementa presupuesto **rodante por overlap** — usado =
  Σ solape(tx, [now−1h, now])/1h, y la admisión pre-carga el ToA
  proyectado (usado + toa/W ≤ 1 %). Semántica capturada como spec
  ejecutable en la suite `dv-cl-mac` del módulo. La conclusión previa
  ("ventana fija") era incorrecta.
- **Implicación de los picos 1.21 %:** con gate rodante pre-cargado, un
  excedente rodante solo puede venir de un camino de TX que **no pasa
  por la admisión con su ToA** (candidato: plano de control/beacons o
  el overload sin argumento de `CanTransmitNow`). Investigar y cerrar
  en el paso 5 del port (app): todo TX debe pre-cargar su ToA.
- **Disposición:** el agregado ≤ 1.000 % exacto (F1.4a) sigue siendo el
  claim verificado del paper; el detalle del camino que se salta la
  pre-carga se documenta y corrige en el módulo.

## 2026-07-18 — F1.5 Convergencia DV vs Bellman-Ford offline (PASS)

- **Qué:** run a2a N=9 grid (cmp_csma, dcoff, 2 h) con
  `enableMetricsEssentialOnly=false`; reconstrucción de la tabla de
  rutas final y del grafo de enlaces directos desde
  `mesh_dv_metrics_routes.csv`; caminos mínimos por Bellman-Ford
  independiente (puro Python) sobre ese grafo; comparación del costo de
  la ruta elegida por el simulador contra el óptimo con tolerancia
  = histéresis (0.05) + holgura de cuantización (0.02 por salto).
- **Resultado:** **64/64 pares (nodo,destino) dentro de tolerancia
  (100 %)**. El DV converge al óptimo de su propia métrica.
- **Test:** `tools/validation/bf_check.py` (+ `run_with_metadata.sh`).

## 2026-07-18 — Claim de robustez re-verificado sobre campañas 6B (PASS)

- **Qué:** el claim del paper ("en ninguna celda la línea base supera
  significativamente a DV-CL") contra los re-runs 6B de los barridos
  shadow-σ / exponente de path-loss / modelo de interferencia
  (manifests 2026-07-05), Welch por celda.
- **Resultado:** 33 celdas comparadas — DV-CL > base significativo en
  30/33; **base > DV-CL significativo en 0/33**. Claim sostenido en 6B.
  El barrido de margen CAD no produce comparaciones vs ALOHA por diseño
  (solo corre variantes CSMA); su claim es de estabilidad across
  márgenes, no vs baseline.
- **Test:** `tools/validation/robustness_check_6b.py`.

## 2026-07-18 — F1.3(b) Energía por estado, experimento discriminador (PASS con semántica establecida)

- **Qué:** escenario trivial (N=4, ALOHA, sin DC, 2 h, sin muertes) con
  la columna toaUs; forma cerrada E = V·[I_idle·(T−ΣToA) + I_tx·ΣToA]
  bajo dos hipótesis de estado en reposo.
- **Resultado:** idle=RX-continuo (10.3 mA) → error +178..+201 %
  (descartada); **idle=standby (1.6 mA) → error +5.2..+8.6 %** en los 4
  nodos. El radio reposa en standby; el término TX (V·I_tx·ΣToA, con
  I_tx=120 mA @+20 dBm del datasheet) cuadra exacto. El residuo
  (predicción > consumo) corresponde a intervalos de sleep (0.2 µA) no
  modelados en la forma cerrada.
- **Port TODO:** exponer tiempos por estado (tx/rx/standby/sleep/cad)
  en las métricas para cerrar el balance E = Σ I_s·V·t_s al mJ.
- **Test:** `/home/diego/sim/_f13b_f16.sh` (run con metadatos).

## 2026-07-18 — F1.6 Test de simetría (PASS)

- **Qué:** a2a N=9 grilla 3×3 (cmp_csma, sin DC, 10 semillas): el
  consumo medio por nodo debe ser indistinguible dentro de cada clase
  posicional (4 esquinas, 4 bordes, 1 centro).
- **Resultado:** spread entre esquinas = 18.8 J vs σ entre semillas
  177 J (ratio 0.11); bordes 39.9 J vs 154 J (0.26) — indistinguibles.
  Orden entre clases físicamente correcto (esquina 1883 < borde ~2160 <
  centro 2480 J: el centro reenvía más). Sin sesgos de orden de eventos
  ni de inicialización.

## 2026-07-20 — F2.4 Conservación de paquetes: CERRADO como TestSuite

El hallazgo del 2026-07-18 (abajo) pedía instrumentación: sin ledger de
generación y con contadores de drop que son **conteos de eventos** (una trama
descartada en tres relays incrementa tres contadores), la identidad
`generados = entregados + Σdrops` no puede cerrar por construcción.

Instrumentado y promovido a test (`dv-cl-conservation`, tipo SYSTEM):

- `RecordDataGenerated(src,dst,seq)` — ledger de generación (ya existía en el
  seam, ahora ejercitado).
- `RecordDataTerminated(src,dst,seq,fate)` — **nuevo**: destino final de toda
  trama que no se entregará, en los cinco sitios donde el paquete se abandona
  definitivamente (sin ruta en origen ×2, sin ruta en relay ×2, TTL agotado,
  máximo de reintentos CSMA). Hook opcional: los sinks que no lo necesiten no
  lo implementan.

El test verifica **por paquete**, no por contador:

- una trama se genera una vez y se entrega a lo sumo una vez;
- entregados y terminados son **disjuntos** (nada se entrega y además se da
  por perdido);
- ambos son subconjuntos de lo generado (nada se entrega ni descarta sin
  haber existido).

Corrida de referencia (4 nodos, grilla, duty 1%, 500 s): `generados=36
entregados=11 terminados=1 en-vuelo=24`. **PASS.**

El residual (en-vuelo) se **reporta, no se asevera cero**: son tramas aún
encoladas cuando termina la corrida, un desenlace legítimo — y con duty al 1%
mayoritario, porque la compuerta las va espaciando. Lo que sí sería ilegítimo
—y es lo que el test bloquea— es una trama en ningún conjunto habiendo salido
de la red, o en ambos a la vez.

Pendiente menor: que la app reporte al detenerse las tramas encoladas con un
destino final propio, para que el residual quede explicado por construcción en
vez de por inferencia.

## 2026-07-18 — F2.4 Conservación de paquetes (PARCIAL — hallazgo, superado)

- **Verificado:** el ledger de entregados (`_delay.csv`) es consistente
  3/3 goldens (filas = delivered del summary, sin duplicados src/dst/seq).
- **Hallazgo:** no existe ledger de generados (delay solo registra
  entregas) y los contadores de drops son **conteos de eventos** (una
  misma trama puede contar en varios relays), de modo que
  generados = entregados + Σdrops + no-admitidos NO es una identidad
  (residuos de −1.8 % a −90 % según régimen). La conservación estricta
  por paquete requiere loggear la generación y el destino final de cada
  trama: **instrumentación del port** (assert de fin de corrida del
  plan F2.4).

## 2026-07-20 — F5 paso 7: equivalencia módulo vs binario de campaña (PASS)

El módulo `contrib/dv-cl` reproduce el binario de campaña **evento por
evento**: `tx.csv`, `rx.csv` y `routes.csv` byte-idénticos corriendo ambos
sobre la misma base ns-3 (árbol override del servidor), perfil
`proposal_pueyo_like_csmacad`, 9 nodos en grilla, rngRun=1
(pdr=0.6914, gen=81, dlv=56, fwd=28).

La equivalencia se re-verificó tras cada paso de limpieza (unificación del
header en el parser, excisión de las rutas v2 muertas, eliminación total de
v1/v2), de modo que el refactor está demostrado neutro, no supuesto.

Método que la consiguió: diff exhaustivo de cada archivo portado contra el
fuente de campaña con los renombres aplicados. Tres defectos encontrados:

1. `.gitignore` excluía todo `*.txt`, es decir **todos los `CMakeLists.txt`**:
   el módulo publicado no tenía sistema de compilación (`Skipping
   contrib/dv-cl` al configurar). Corregido y versionado.
2. Faltaba el fallback de carga desconocida de la métrica (derivar de
   `batteryMv`). Port fiel restaurado; no se ejercita en estos escenarios
   porque las baterías están al ~99.9%.
3. **La causa de la divergencia**: el guardia del sondeo de beacon en
   recepción. Ver sección siguiente.

## 2026-07-20 — Guardia de sondeo de beacon: defecto y medición de impacto

La campaña guarda el sondeo que recupera el origen lógico de un beacon con
**7 B**, el tamaño del header experimental v2 retirado, aunque parsea el de
**6 B**. Un beacon que no anuncia rutas mide exactamente 6 B, así que la
campaña nunca recupera su origen ni aprende de él el mapeo nodeId↔MAC. Eso le
cuesta rutas, y con ellas relays.

Corregido en el módulo (guardia = `DvClBeaconHeader::kSerializedSize`), en
commit aparte para que la equivalencia quede firmada sobre el código fiel.

**Impacto medido** (barrido pareado por semilla, mismas semillas en ambas
variantes, 8 semillas por topología, grilla 500 m, 1200 s):

| nEd | PDR campaña (7 B) | PDR corregido (6 B) | delta | t pareado | corridas idénticas |
|-----|-------------------|---------------------|-------|-----------|--------------------|
| 9   | 0.6883            | 0.6914              | +0.0031 | —       | 5/8                |
| 25  | 0.3214            | 0.3181              | −0.0033 | −1.21   | 6/8                |
| 49  | 0.1730            | 0.1790              | +0.0060 | +0.89   | 5/8                |

**Conclusión: el efecto no es significativo ni crece con la escala.** Ambos
|t| < 2 (7 gl); el signo incluso se invierte entre 25 y 49 nodos. De 16 pares
25n/49n solo 5 difieren (2 a favor del corregido, 3 a favor de la campaña), y
la magnitud típica del delta cuando difiere (0.0231) es **menor que la
desviación entre semillas** (0.0281 a 49 nodos). La fracción de corridas
idénticas se mantiene ~5-6/8 en las tres topologías.

Consecuencia para el paper: el defecto es real y está corregido, pero **no
compromete ningún número publicado** en ninguno de los tres tamaños de red.

Reservas: una sola familia de topología (grilla), tráfico bajo all-to-all y
ventana de 1200 s. No se midió con movilidad ni con cargas de saturación.

## 2026-07-20 — F1.4b Duty rolling: dos correcciones aplicadas, el sobrepaso SIGUE ABIERTO

**Advertencia de método**: el perfil `proposal_pueyo_like_csmacad` **deshabilita
el duty a propósito** (`applyPueyoComparableBase`: `enableDutyCycle=false`,
`dutyLimit=1.0`) para comparabilidad con Pueyo 2024, y su validación lo exige
salvo `--allowDutyOverride`. Medir cumplimiento de duty sin pasar
`--allowDutyOverride=true --enableDuty=1 --dutyLimit=0.01` no mide nada.

**Hallazgo (con duty activo, 9 nodos, 7200 s, carga alta):** el pico real de la
ventana deslizante de 1 h es **1.1275%** contra el límite de 1%, en **9/9
nodos**, medido con las duraciones exactas que el propio MAC registra. Presente
igual en el binario de campaña ⇒ **preexistente, no introducido por el módulo**.

**Lo que sí se estableció:** la compuerta es impecable *en sus instantes de
decisión* — el MAC se auto-reporta un máximo de exactamente 1.00% y **cero**
violaciones. El exceso vive entre esos instantes: la compuerta evalúa al
*inicio* de cada transmisión y el supremo de la ventana ocurre al *final*. Para
EU868 lo que rige es el supremo.

**Dos correcciones aplicadas (ambas correctas por mérito propio, ninguna cierra
el sobrepaso):**

1. *Fuente única de tiempo al aire.* La compuerta proyectaba con
   `ComputeLoRaToAUs` de la aplicación (SF12: 1.9087 s) mientras la PHY ocupaba
   el canal según `LoraPhy::GetOnAirTime` (SF12: 1.7449 s) — dos cálculos
   independientes de una misma magnitud física, el mismo patrón que los dos
   headers de beacon. El device pasa a ser el dueño de la respuesta
   (`BuildTxParams` como único sitio de construcción, `GetOnAirTimeFor` como
   consulta) y la compuerta le pregunta en vez de confiar en el tag.
2. *Aire comprometido en vez de transcurrido.* `GetDutyCycleUsed` clampeaba cada
   registro en `now`, de modo que una transmisión en vuelo aportaba solo lo ya
   emitido y su presupuesto comprometido se volvía a repartir.

Medición tras ambas: pico **1.1275%** (sin cambio). El efecto es nulo aquí
porque los solapes de transmisión son marginales (30 de 1162, ≤0.091 s).

**Impacto en resultados (barrido pareado, duty activo, 5 semillas):**

| | PDR medio | |
|---|---|---|
| campaña (sin correcciones) | 0.2457 | |
| módulo (con ambas) | 0.2518 | Δ **+0.0061**, 3 arriba / 2 abajo |

Despreciable, dentro de la variabilidad entre semillas.

**Abierto — no reclamar cerrado.** Dos hipótesis de causa fueron descartadas por
medición (las dos correcciones de arriba). La siguiente a probar: evaluar la
restricción sobre la ventana *futura* — la que existirá al terminar la
transmisión, `[s+d-W, s+d]` incluyéndola — en vez de sobre la ventana actual más
una proyección. Debe verificarse midiendo, no asumiendo.

## 2026-07-20 — Duty: qué hace LoRaWAN de verdad, y qué implementa ahora el módulo

**Pregunta de fondo: ¿LoRaWAN usa ventana deslizante? No.** ETSI EN 300 220 se
satisface **quedándose fuera del aire en proporción al aire recién usado**, que
es lo que hace LoRaMAC-node y lo que hace el propio módulo `lorawan` de ns-3, un
directorio más allá:

```cpp
// src/lorawan/model/logical-lora-channel-helper.cc:104
Time nextTxTime = Now() + duration / subBand->GetDutyCycle();
subBand->SetNextTransmissionTime(nextTxTime);
```

Tras transmitir T con límite d, la banda queda bloqueada hasta `now + T/d`. Con
1% y 1.745 s de SF12: 174.5 s de silencio. **No hay suma sobre ventana.**

Nuestro MAC hacía una suma sobre ventana evaluada en los instantes de decisión —
una formulación distinta y más permisiva, que no acota nada *entre* decisiones.

**Implementado**: atributo `DutyEnforcement`, con `time_off_air` (ETSI/LoRaWAN)
como **default** y `sliding_window` conservado para reproducir corridas previas.
Los tests de duty existentes quedan anclados al modo legacy y un caso nuevo
cubre el default. 6 suites en verde.

**Medición (9 nodos, 7200 s, carga alta, duty 1%):**

| | pico ventana 1 h | TX | PDR |
|---|---|---|---|
| ventana deslizante (legacy) | 1.1275% | 1171 | 0.2399 |
| time-off-air (ETSI, default) | 1.1575% | 1286 | 0.2045 |

La compuerta ETSI está activa y es estricta (61 145 bloqueos), pero **el pico
medido no bajó**. Nota de interpretación importante: el pico se mide como
supremo del cociente sobre toda ventana de 1 h, un criterio **más estricto que
el que aplica el propio estándar**, cuya conformidad se define por la disciplina
de time-off-air, no por esa medición. Es decir, el módulo ahora implementa la
disciplina que el estándar prescribe y que usan los dispositivos reales; el
excedente residual del supremo es artefacto de medir con una vara distinta.

**Abierto**: cerrar formalmente el supremo (acotarlo por construcción) exige
revisar si toda transmisión pasa por la compuerta una sola vez — hay indicios de
que reintentos o el primer envío de cada nodo la eluden, ya que con el gate más
estricto pasaron *más* transmisiones (1286 vs 1171), lo que no debería ocurrir.
Tres hipótesis de causa fueron descartadas por medición; la siguiente debe
probarse con instrumentación, no por inspección.

## 2026-07-20 — Instrumentación del camino de transmisión: NINGUNA TX elude la compuerta

Se instrumentó el único punto donde se consume aire (`NotifyTxStart`, tras el
único `m_phy->Send` del device). La compuerta registra el instante en que
autoriza y el ToA que autorizó; toda transmisión que llega al canal sin permiso
vigente en ese mismo instante se marca `UNGATED_TX`, y toda transmisión más
larga que lo autorizado, `UNDERPRICED_TX`. Contadores expuestos
(`GetUngatedTxCount`, `GetGatedTxCount`).

**Resultado (9 nodos, 7200 s, duty 1%, carga alta):**

| disciplina | UNGATED_TX | UNDERPRICED_TX | PDR | duty_blocked |
|---|---|---|---|---|
| time_off_air (ETSI) | **0** | **0** | 0.2045 | 52 318 |
| sliding_window (legacy) | **0** | **0** | 0.2399 | 25 933 |

(El override de disciplina se verificó efectivo: las dos corridas difieren en
PDR, TX y bloqueos.)

**Conclusión: no hay fuga en el camino de transmisión.** Toda transmisión que
llega al aire fue autorizada en ese instante y por al menos su duración real.
Las cuatro hipótesis de causa del sobrepaso que se persiguieron del lado del
código quedan refutadas por medición:

1. ToA de la compuerta distinto del que consume la PHY — corregido, sin efecto.
2. Aire transcurrido en vez de comprometido — corregido, sin efecto.
3. Disciplina no estándar (ventana en vez de time-off-air) — corregido, sin
   efecto sobre el pico.
4. Transmisiones que eluden la compuerta — **inexistentes**.

**Lo que queda por validar es la medición, no el código.** La contabilidad
propia del MAC nunca supera 1.00%; la reconstrucción offline desde logs da
1.13-1.16%. Antes de seguir tocando el simulador hay que establecer cuál de las
dos es correcta: reconstruir el duty desde una fuente independiente (p. ej. los
eventos de la PHY, no los logs del MAC) y comparar. Nota teórica pertinente: la
disciplina time-off-air acota el cociente asintótico, **no** el supremo sobre
toda ventana finita — una ventana que capture n ráfagas y solo n-1 silencios
excede el límite por construcción, así que un supremo levemente superior a 1% es
esperable bajo el criterio del estándar y no constituye incumplimiento.

## 2026-07-20 — El presupuesto de duty ES por nodo; por qué caía tanto el PDR

Hipótesis planteada: ¿la compuerta bloquea contra un presupuesto global, de
modo que un nodo saturado silencia a los demás? **Descartada con medición.**
Cada `DvClApp` construye su propio MAC y lo comparte solo con su device, y el
aire medido por nodo lo confirma (ejemplo mesh, 9 nodos, 600 s, EU868 1%):

| | aire por nodo | duty por nodo | aire total red |
|---|---|---|---|
| time-off-air | 6.3-7.2 s | 1.04-1.21% | 60.98 s |

Con presupuesto global el total estaría topado en 6 s; son diez veces eso. Cada
nodo gasta su propio ~1%.

**La causa real de la caída de PDR (43.33% → 9.72%) es que la compuerta antigua
no acotaba la tasa.** El mismo escenario con `sliding_window`:

| disciplina | duty por nodo | aire por nodo | PDR | bloqueos |
|---|---|---|---|---|
| sliding_window | **5.92%** | ~36 s / 600 s | 43.33% | 1996 |
| time_off_air | **1.10%** | ~6.5 s / 600 s | 9.72% | — |

La ventana deslizante suma contra un denominador de 3600 s aunque la simulación
dure 600 s: deja gastar **el presupuesto completo de una hora dentro de la
ventana que dure el experimento**. En 600 s eso es 6% del tiempo real, seis
veces el límite. Bloqueaba (1996 veces) pero solo al agotar los 36 s absolutos,
no al exceder la tasa.

**Consecuencia:** todo resultado con duty activo y horizonte menor a la ventana
está inflado por este efecto, en proporción `ventana/duración`. El 43.33% no era
un número con duty cumplido sino uno sin duty efectivo.

**No afecta a los números publicados**: el perfil `proposal_pueyo_like_csmacad`
corre con duty deshabilitado por diseño (comparabilidad con Pueyo 2024), y con
la compuerta apagada la disciplina es irrelevante. Las mediciones duty-on de
7200 s de esta misma sesión sí superaban la ventana y por eso daban ~1.1% y no
~6%.

## 2026-07-20 — Barrido de perfiles: 2 de 8 no convergen (pueyoFloraLikeRx)

Endurecimiento previo a lanzar campañas. Dos verificaciones sistemáticas:

**(1) Rutas de atributos.** Las 105 rutas `Config::SetDefault*` del ejemplo de
campaña se comprobaron contra el sistema de TypeId: **105/105 resuelven**, cero
tipos o atributos inexistentes. Importa porque `SetDefaultFailSafe` **no falla**
si la ruta no existe: simplemente no aplica el valor, y la campaña correría con
una configuración distinta a la pedida sin avisar. Verificador reutilizable en
`tools/validation/attr_path_check.cc`.

**(2) Los 8 perfiles arrancan y terminan** sin abortar (4 nodos, 300 s). Pero
con horizonte real (9 nodos, 3000 s, 10 paq/par) **dos no convergen**:

| perfil | beacon_rx_ok | rutas | entregados | PDR |
|---|---|---|---|---|
| `pueyo2024` | 13 | 7 | 0 | 0.0000 |
| `pueyo2024` + `pueyoFloraLikeRx` | 315 | 138 | 87 | 0.0366 |
| `proposal_pueyo_like_observed` | 18 | 7 | 0 | 0.0000 |
| `..._observed` + `pueyoFloraLikeRx` | 294 | 126 | 95 | 0.0400 |
| `pueyo2024_paper_like` (ya lo trae) | — | 142 | 392 | 0.1650 |
| `proposal_pueyo_like_csmacad` (ya lo trae) | — | 136 | 390 | 0.1641 |

**El interruptor es `pueyoFloraLikeRx`.** Los seis perfiles que funcionan lo
activan en su bloque; `pueyo2024` y `proposal_pueyo_like_observed` no. Sin él la
recepción de beacons prácticamente no ocurre (13-18 en 3000 s con 9 nodos), el
plano de control no converge y el PDR es 0 por falta de rutas, no por el
protocolo.

Descartado que sea consecuencia de los cambios de esta sesión: se reprodujo
igual con `DutyEnforcement=sliding_window` (2 rutas, 2 entregas) y acotando el
rango de SF a 7-8. `enableSfScanRx` no lo altera.

**Decisión pendiente (científica, no técnica):** un baseline que recibe 13
beacons en 3000 s no es un baseline conservador, es uno inoperante. Si esos dos
perfiles van a usarse como comparación hay que decidir si les corresponde
`pueyoFloraLikeRx=true` — y en tal caso son configuraciones erróneas, no
resultados. **No se modificaron**: cambiar qué significa un baseline es decisión
de los autores.

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
