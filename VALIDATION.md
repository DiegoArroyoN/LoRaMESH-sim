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

## 2026-07-20 — Endurecimiento: warnings y robustez de parámetros

**Compilación con avisos agresivos** (`-Wall -Wextra -Wshadow`): el módulo queda
**limpio**. Los únicos avisos eran `unused-parameter` (cuatro del no-op por
defecto del hook opcional del sink, cuatro de métodos de la interfaz NetDevice
que este device no usa), marcados `[[maybe_unused]]`. **Nada sustantivo**: sin
shadowing, sin lecturas no inicializadas, sin comparaciones de signo, sin
destructores no virtuales. Los avisos que restan provienen de cabeceras del
propio ns-3 (`callback.h`, `ptr.h`, `buffer.h`).

**Barrido de robustez de parámetros** (19 combinaciones, perfil
`proposal_pueyo_like_csmacad`):

| familia | casos | resultado |
|---|---|---|
| tamaño de red | nEd = 1, 4, 9, 16, 25 | ok (incluye el nodo único) |
| tamaño inválido | nEd = 2, 3 | **aborta con mensaje**: "nEd debe ser cuadrado perfecto" |
| separación | 1 m, 10 m, 400 m, 3 km, 20 km | ok (incluye sin conectividad) |
| carga | low, medium, high, saturation | ok |
| borde | 0 paquetes/par, línea, aleatorio | ok |
| borde inválido | stopSec < dataStartSec | **aborta con mensaje** explícito |

**Los tres abortos son validación de entrada deliberada**, no fallos: el binario
rechaza configuraciones imposibles de inmediato y diciendo por qué, que es el
comportamiento deseable en un lote — la corrida inválida muere sola, ruidosa y
sin contaminar las demás.

Conclusión operativa: el simulador tolera todo el rango razonable de parámetros
y falla rápido y claro fuera de él.

## 2026-07-20 — Ensayo de lote completo (24 corridas): artefactos sanos

Matriz 6 perfiles x {9,16} nodos x 2 semillas, horizonte 2000 s, métricas
completas (no essential-only). Verificado por corrida: código de salida, número
de CSV, summary parseable, PDR en [0,1], ausencia de NaN/Inf en todo el JSON, y
consistencia de ancho de columnas en cada CSV.

**24/24 limpias**: `rc=0`, 9 CSV, summary válido, sin valores no finitos. La
generación es idéntica por (nodos, semilla) entre perfiles — 1476/1478 con 9
nodos, 2650/2631 con 16 — lo que confirma que el tráfico ofrecido no depende del
perfil y las comparaciones parten de la misma base.

**Anomalía de formato (menor):** `mesh_dv_metrics_lifetime.csv` contiene **dos
tablas en un archivo** separadas por línea en blanco (resumen `metric,value_s` y
log de muertes `timestamp,nodeId,energyFrac,reason`). No es corrupción, pero un
`read_csv` estándar lo mal-parsea. Hoy **ningún script del repo lo lee**, así que
no rompe nada; queda anotado para quien añada ese análisis.

**Hallazgo metodológico: los perfiles NO son comparables en PDR entre sí.**

| perfil | bloqueos de duty | PDR (9 nodos) |
|---|---|---|
| `proposal_pueyo_like` | 16 102 | 0.0339 |
| `proposal_pueyo_like_aloha` | 0 | 0.1775 |
| `proposal_pueyo_like_csmacad` | 0 | 0.1741 |
| `pueyo2024_paper_like` | 0 | 0.1653 |
| `pueyo2024_paper_like_csmacad` | 0 | 0.1612 |
| `csmacad_free_backoff` | 0 | 0.1768 |

**Uno de seis paga duty cycle; cinco no** (heredan `enableDutyCycle=false` de
`applyPueyoComparableBase`). La diferencia de ~5x en PDR es el costo del 1%, no
una diferencia de encaminamiento. Cualquier tabla que ponga estos perfiles lado
a lado sin decirlo estará atribuyendo al protocolo lo que es régimen regulatorio.

Descartado que la disciplina de duty lo explique: `proposal_pueyo_like` entrega
**más** con `time_off_air` (50) que con `sliding_window` (42), y los perfiles sin
duty dan resultado idéntico bajo ambas.

## 2026-07-20 — Perfiles corregidos: 8/8 convergen, resultados interpretables

Aplicado `pueyoFloraLikeRx=true` a `pueyo2024` y `proposal_pueyo_like_observed`
(decisión de Diego), más la validación `NS_ABORT_MSG_IF(!pueyoFloraLikeRx, ...)`
que los otros seis perfiles ya tenían, para que no se vuelva a perder.

| perfil | beacon_rx_ok | rutas | entregados | PDR | régimen |
|---|---|---|---|---|---|
| `pueyo2024` | 228 (era 13) | 140 (era 7) | 56 (era 0) | 0.0379 | ALOHA puro, ToA-only, SF 7-12 |
| `proposal_pueyo_like_observed` | 199 (era 18) | 134 (era 7) | 60 (era 0) | 0.0407 | **duty 1%** |
| `proposal_pueyo_like` | 402 | 143 | 50 | 0.0339 | **duty 1%** |
| `proposal_pueyo_like_aloha` | 198 | 141 | 262 | 0.1775 | sin duty |
| `proposal_pueyo_like_csmacad` | 211 | 134 | 257 | 0.1741 | sin duty |
| `pueyo2024_paper_like` | 176 | 142 | 244 | 0.1653 | sin duty |
| `pueyo2024_paper_like_csmacad` | 182 | 141 | 238 | 0.1612 | sin duty |
| `csmacad_free_backoff` | 165 | 142 | 261 | 0.1768 | sin duty |

Los ocho convergen y ninguno de los seis previos se alteró. Los números pasan a
ser interpretables: `pueyo2024` queda bajo (0.038) **por ser el baseline débil
por diseño** —ALOHA, métrica solo-ToA, rango de SF completo— y no por avería; los
dos perfiles con duty al 1% quedan en 0.034-0.041 pagando el régimen.

**Al tabular resultados, separar por régimen**: tres perfiles operan con duty y
cinco sin él, y la brecha de ~4-5x entre ambos grupos es regulatoria, no de
encaminamiento (confirmado en la entrada anterior).

## 2026-07-20 — Campaña completa post-endurecimiento (360 corridas)

Primera campaña sobre el simulador endurecido: 8 perfiles, {9,25,49} nodos, 10
semillas, 7200 s de horizonte (dos ventanas de duty), datos desde 600 s, PDR
medido en los últimos 1800 s, métricas completas. **360/360 corridas sin fallo**,
15 GB. Índice en `tools/validation/campaign_20260720_index.csv`.

**PDR medio ± desv (10 semillas), perfiles sin duty:**

| perfil | n=9 | n=25 | n=49 |
|---|---|---|---|
| `pueyo2024_paper_like_csmacad` | 0.1706±0.021 | 0.0778±0.003 | 0.0392±0.002 |
| `pueyo2024_paper_like` | 0.1688±0.021 | 0.0771±0.004 | 0.0385±0.002 |
| `proposal_pueyo_like_csmacad` | 0.1679±0.020 | 0.0771±0.003 | 0.0381±0.002 |
| `csmacad_free_backoff` | 0.1674±0.020 | 0.0768±0.002 | 0.0386±0.002 |
| `proposal_pueyo_like_aloha` | 0.1657±0.021 | 0.0767±0.004 | 0.0376±0.002 |
| `pueyo2024` | 0.0733±0.031 | 0.0114±0.002 | 0.0043±0.001 |

**Hallazgo 1 — los cinco perfiles superiores son indistinguibles entre sí.** El
rango completo (0.1657-0.1706 en n=9) cabe holgadamente dentro de una
desviación (±0.020). Solo `pueyo2024` —el baseline débil por construcción— se
separa. Con esta configuración **la propuesta no se distingue de los baselines
en PDR**; si la tesis afirma superioridad, no es en este eje ni en este régimen,
y habrá que buscarla en energía, vida útil o latencia, o en un régimen donde el
duty muerda.

**Hallazgo 2 — la disciplina de duty conformante rinde MEJOR:**

| perfil | disciplina | n=9 | n=25 | n=49 |
|---|---|---|---|---|
| `proposal_pueyo_like` | time_off_air | **0.0304** | **0.0118** | **0.0041** |
| | sliding_window | 0.0191 | 0.0054 | 0.0023 |
| | fixed_window | 0.0194 | 0.0066 | 0.0032 |
| `proposal_pueyo_like_observed` | time_off_air | **0.0374** | **0.0194** | **0.0089** |
| | sliding_window | 0.0182 | 0.0060 | 0.0026 |
| | fixed_window | 0.0163 | 0.0054 | 0.0024 |

Con **el mismo presupuesto de aire**, `time_off_air` entrega 1.6x-2.3x más que
las otras dos lecturas de la norma, consistente en ambos perfiles y las tres
escalas. La explicación mecánica: espacia las transmisiones de forma continua en
vez de permitir ráfagas seguidas de bloqueos largos, y las ráfagas colisionan
consigo mismas. `sliding_window` y `fixed_window` quedan parejas entre sí, ambas
propensas a ráfaga.

Resultado aprovechable para el paper: **interpretar el duty cycle como
espaciado continuo (lo que hacen los dispositivos reales) no es solo lo
correcto normativamente, también es lo que mejor rinde** — la conformidad no
cuesta rendimiento, lo gana.

## 2026-07-20 — HALLAZGO CRÍTICO: la campaña no ejercita el aporte de la tesis

Análisis multi-eje de las 360 corridas (energía, latencia, overhead, balance de
carga, vida útil), no solo PDR.

**Los cinco perfiles comparables son indistinguibles en TODOS los ejes**, no solo
en PDR (n=9, media de 10 semillas):

| perfil | PDR | J/entregado | latencia media | overhead | dispersión carga |
|---|---|---|---|---|---|
| `pueyo2024_paper_like_csmacad` | 0.1706 | 0.9024 | 0.178 s | 0.161 | 0.0055 |
| `proposal_pueyo_like_csmacad` | 0.1679 | 0.9081 | 0.178 s | 0.163 | 0.0059 |
| `csmacad_free_backoff` | 0.1674 | 0.9155 | 0.175 s | 0.161 | 0.0062 |
| `pueyo2024_paper_like` | 0.1688 | 0.9105 | 0.174 s | 0.161 | 0.0055 |
| `proposal_pueyo_like_aloha` | 0.1657 | 0.9174 | 0.172 s | 0.165 | 0.0054 |
| `pueyo2024` (baseline débil) | 0.0733 | **19.45** | 1.87 s | 0.187 | **0.0417** |

**Causa mecánica identificada.** El término energético es delta*Psi(SoC), y
`Psi(b) = 0` para `b >= EnergyHi = 0.50`. En las 360 corridas:

- **0 de 240 corridas tienen algún nodo muerto** (`fnd_s = -1` en todas).
- La carga restante mínima es **~0.967**.

Es decir: **todos los nodos permanecen siempre por encima de 0.50, luego Psi = 0
para todos en todo instante, y la métrica compuesta se reduce a alpha*ToA +
beta.** El término que constituye el aporte de la tesis **nunca se activa**.

La propuesta no es indistinguible de los baselines de solo-ToA porque el aporte
falle: **en este régimen la propuesta ES solo-ToA**. La campaña no puede, por
construcción, medir lo que la tesis afirma.

**Consecuencia:** ningún resultado de esta campaña —ni a favor ni en contra— dice
nada sobre el aporte energético. Las comparaciones válidas que sí deja son las de
disciplina de duty (entrada anterior) y la del baseline débil `pueyo2024`, que
difiere en cuatro factores simultáneos (ALOHA, solo-ToA, rango SF completo,
recepción no-FLoRa) y por tanto **no aísla ninguna contribución**.

**Qué haría falta para probar la tesis:** que los nodos crucen los umbrales
`EnergyLo=0.20` y `EnergyHi=0.50`. A la tasa observada (~3% de carga por 2 h) se
requerirían ~33 h simuladas (~120 000 s) para llegar al 50%. Con 12 s de reloj
por corrida de 7200 s, una campaña de vida útil costaría ~200 s por corrida:
**perfectamente viable**. La alternativa más rápida es dimensionar la batería
inicial para que arranque cerca de los umbrales.

## 2026-07-20 — CAMPAÑA DE VIDA ÚTIL DETENIDA: la métrica no ve la batería

Al calibrar la campaña de vida útil apareció la causa de fondo del resultado
nulo. **No es solo que el horizonte sea corto.**

**Defecto 1 — el SoC inicial heterogéneo nunca llega a la métrica.**
`DvClEnergyRegistry::RegisterNode(NodeId)` inicializa `remainingMah =
m_capacityMah`, capacidad completa, y se le invoca sin parámetro de carga. El
SoC aleatorio U[60,100]% que la campaña asigna va al `BasicEnergySource` de ns-3,
**no al registro**, que es el modelo que `DvClApp::GetRemainingEnergyJ()` lee y
que alimenta `Psi(SoC)`. Evidencia: `mesh_dv_metrics_energy.csv` reporta
`energyInitialJ = 3887.97` (capacidad llena) para **todos** los nodos, mientras
el log del framework dice "Node 8 initial SOC: 75.1% (2919.8J / 3888.0J)".

**Defecto 2 — dos capacidades para una batería.** La fracción se calcula como
`remainingJ / m_batteryFullCapacityJ`, donde el numerador viene del registro y el
denominador es un atributo distinto de la app (default 38880 J). Pasar
`--batteryFullCapacityJ=120` cambia el divisor pero no la capacidad del registro,
de modo que la fracción sale `3571/120 = 29.8` clampeada a **1.0**: el flag no
achica la batería, **corrompe la medición**. Cualquier corrida que lo use produce
energía sin sentido.

Es el mismo patrón que los dos headers de beacon y los dos cálculos de ToA: **dos
fuentes de verdad para una magnitud física**.

**Consecuencia para la tesis:** el término `delta*Psi(SoC)` recibe ~1.0 para todos
los nodos en todo instante, por construcción y no por el horizonte. La campaña de
360 corridas no midió el aporte, y **una campaña de vida útil lanzada sobre este
código tampoco lo mediría**. Lanzamiento detenido.

**Fix requerido antes de cualquier campaña energética:**

1. `RegisterNode(id, initialCharge)` — que la carga inicial heterogénea entre al
   registro.
2. Que la fracción divida por la capacidad del **propio registro**, eliminando el
   atributo paralelo de la app (fuente única).

Ambos cambian resultados: son corrección, no ajuste.

## 2026-07-20 — Campaña de vida útil: el aporte energético, MEDIDO

Tras el fix de energía (SoC heterogéneo llega al registro, fracción de fuente
única), primera campaña que **puede** medir delta*Psi(SoC). 60 corridas, {9,25,49}
nodos, 10 semillas, horizonte 300 000 s, SoC inicial U[15,35]% para que la red
llegue a agotarse. 60/60 sin fallo. Índice en
`tools/validation/lifetime_20260720_index.csv`.

**Comparación que aísla el aporte**: `composite_score` vs `toa_only`, idénticas
en todo lo demás (ambas pueyo-comparable + CSMA/CAD; difieren SOLO en el término
energético de la métrica). FND = primer nodo muerto, T50 = mitad de la red
muerta.

| n | métrica | FND (s) | T50 (s) | PDR | composite vs toa |
|---|---|---|---|---|---|
| 9 | composite | 89691±7760 | 139929±16070 | 0.1693 | FND +1.3%, T50 +0.6% |
| 9 | toa_only | 88575±7497 | 139125±16071 | 0.1698 | |
| 25 | composite | 60163±7255 | 107512±10161 | 0.0773 | FND +5.6%, T50 +3.6% |
| 25 | toa_only | 56953±7378 | 103775±9752 | 0.0779 | |
| 49 | composite | 36985±2035 | 75715±6078 | 0.0378 | FND +4.8%, **T50 +6.6%** |
| 49 | toa_only | 35300±2064 | 71042±5708 | 0.0392 | |

**Lectura honesta:**

1. **El aporte existe y crece con la densidad.** El término energético alarga la
   vida de la red, y el efecto es mayor cuanto más grande la red: T50 pasa de
   +0.6% (9 nodos) a +6.6% (49 nodos). Tiene sentido mecánico: con más nodos hay
   más rutas alternativas que evitar los de batería baja, así que el balanceo
   tiene de dónde elegir.

2. **En 9 nodos el efecto es indistinguible del ruido** (+1.3% FND con σ≈8%). La
   red es demasiado pequeña para que el balanceo importe. La topología de la
   campaña principal (9 nodos, sin agotamiento) no habría mostrado nada aunque
   el fix hubiera estado — doble razón por la que no aparecía.

3. **Cuesta un poco de PDR** (−0.3% a −3.6%, creciente con n): desviar tráfico de
   los nodos con mejor enlace hacia los de más batería usa rutas algo peores.
   Es el compromiso esperado vida-útil/entrega, y a 49 nodos es medible: **+6.6%
   de T50 por −3.6% de entregados**.

**Este es el resultado defendible de la tesis**, y es un compromiso, no una
victoria en todos los ejes: la métrica compuesta **compra vida de red con una
fracción de PDR**, y el trato mejora con la densidad. Reportarlo como tal —con
FND/T50 y el costo de PDR lado a lado— es más fuerte que afirmar superioridad
uniforme, que los datos no respaldan.

## 2026-07-20 — Barrido de densidad 81/100: el aporte se estabiliza, no crece sin fin

Extensión de la campaña de vida útil a 81 y 100 nodos (40 corridas, 10 semillas,
misma config de agotamiento). 40/40 sin fallo. Solo se guardó el summary por
corrida (CSV por-evento de 2.5 GB borrados al vuelo). Índice en
`tools/validation/lifetime_20260720_density_index.csv`.

**Serie completa del aporte (composite_score vs toa_only, delta relativo):**

| n | dFND | dT50 | dPDR | significativo T50 |
|---|---|---|---|---|
| 9 | +1.3% | +0.6% | -0.3% | no (ruido) |
| 25 | +5.6% | +3.6% | -0.8% | no |
| 49 | +4.8% | +6.6% | -3.6% | no |
| 81 | +7.7% | +5.8% | -4.1% | **sí** |
| 100 | +7.4% | +6.2% | -5.7% | **sí** |

**El aporte se satura, no diverge.** La ganancia de vida útil sube hasta ~49-81
nodos y luego se **estabiliza** en torno a +6-8% (FND) y +6% (T50); no sigue
creciendo. Y solo a partir de 81 nodos el efecto sobre T50 **supera la
desviación entre semillas** — antes de esa densidad es real en la media pero
indistinguible del ruido corrida a corrida.

**El costo de PDR, en cambio, sí crece monótonamente** con la densidad: -0.3% ->
-5.7%. El compromiso empeora en el eje de entrega justo cuando mejora en el de
vida útil.

**Lectura consolidada para el paper:**

- El término energético compra vida de red, y el beneficio es **estadísticamente
  sólido solo en redes densas (>=81 nodos)**, donde hay suficientes rutas
  alternativas para que el balanceo actúe.
- Es un **compromiso Pareto**, no una mejora gratuita: a 100 nodos, +7.4% FND /
  +6.2% T50 a cambio de -5.7% PDR. Cuál extremo conviene depende de si la
  aplicación prioriza cobertura temporal (vida de red) o caudal (PDR).
- El punto óptimo de demostración es ~81-100 nodos: densidad suficiente para
  significancia, sin que el costo de PDR domine.

Este es el resultado central defendible de la tesis, y su fuerza está en
enunciarlo como compromiso cuantificado, no como superioridad.

## 2026-07-20 — RESUELTO: 0.20/0.50 SON los umbrales de la tesis; el FSD estaba obsoleto

Diego aportó la ecuación de la tesis (Ec. Ψ):

```
Ψ(b) = 0                                    si b ≥ b_hi
     = Ψ_max·((b_hi − b)/(b_hi − b_lo))^p    si b_lo < b < b_hi
     = Ψ_max                                 si b ≤ b_lo
```

con `b_lo = 0.20`, `b_hi = 0.50`, `p = 2`, `Ψ_max = 1`.

**El código coincide exactamente** —mismos umbrales, mismo exponente, mismo
Ψ_max, mismos tres tramos en el mismo orden—, de modo que la discrepancia
detectada abajo se resuelve a favor del código: **`FSD_LLD` §4.3.2 estaba
obsoleto desde febrero de 2026** y quedó corregido.

**Consecuencias:**

1. **Las campañas de hoy midieron la métrica correcta.** Los resultados de vida
   útil y densidad (aporte que satura en ~+6-8% FND / +6% T50 a partir de 81
   nodos, a cambio de −4 a −6% de PDR) **se sostienen**.
2. **El hallazgo del término inactivo es una propiedad real de la tesis**, no un
   defecto: por encima de `b_hi` el término es exactamente cero **por diseño**.
   La lectura correcta no es "la campaña estaba mal" sino **"la métrica energética
   solo actúa cuando algún vecino baja del 50%"**, lo cual es una afirmación
   publicable sobre el régimen de aplicabilidad del aporte.
3. Ecuación fijada por test (`dv-cl metric: Psi(b) matches Eq. (psi) of the
   thesis`) contra los valores exactos, incluida la monotonía de la rampa.

**Pesos confirmados (2026-07-20):** α/β/δ = **0.60/0.15/0.25** son los de la
tesis. El código ya los tenía; el FSD (0.40/0.30/0.30 con salto normalizado)
queda retirado. Fijados por test `dv-cl metric: alpha, beta and delta are the
thesis weights`, tanto como defaults de atributo como por descomposición del
compuesto completo.

Con esto **la métrica está enteramente anclada a la tesis**: la ecuación Ψ y los
tres pesos, cada uno con su test. Cualquier deriva futura entre código y tesis
falla en CI en vez de desplazar silenciosamente todos los costos de ruta.

**Trabajo futuro acordado:** barrido de sensibilidad de α/β/δ buscando el punto
que maximiza PDR. Nota de diseño para cuando se haga: el barrido tendrá una
tensión intrínseca, porque las campañas de hoy midieron que el término
energético **cuesta** PDR (−0.3% a −5.7% según densidad). Maximizar PDR
probablemente empuje δ hacia 0, es decir hacia la métrica solo-ToA. El barrido
por tanto no debería optimizar PDR aislado sino **exhibir la frontera de Pareto
PDR/vida-útil**, que es la contribución real.

## 2026-07-20 — (superado) El diseño documentaba otra fórmula

Pregunta: ¿son 0.20/0.50 los umbrales que defiende la tesis? **No se puede
confirmar desde el repositorio, y la evidencia disponible apunta en contra.**

La única especificación escrita en el repo, `FSD_LLD_Simulador_LoRaMESH.md`
(fecha: febrero 2026; último cambio en git: 2026-03-05), documenta en §4.3.2 una
métrica **distinta de la implementada**:

| | FSD_LLD (documento) | código actual (`dv-cl-metric`) |
|---|---|---|
| pesos | alpha=0.40, beta=0.30, delta=0.30 | alpha=0.60, beta=0.15, delta=0.25 |
| término de saltos | `beta * min(hops/h_max,1)`, h_max=10 | `beta` constante por salto |
| Psi(b) | **`1 - b^2`** (suave, sin umbrales) | **por tramos** con b_lo=0.20, b_hi=0.50, p=2 |

El FSD **no menciona en ningún punto** los pesos implementados ni la forma por
tramos ni umbral alguno. Es decir: **los umbrales 0.20/0.50 no aparecen en la
especificación escrita**; provienen del árbol de campaña (`override 2026-05-30`),
según la propia nota de `dv-cl-metric.h`, que a su vez afirma que esa forma es la
que "coincide con el paper". **Ambas afirmaciones no pueden ser ciertas a la vez**
y el paper no está en el repositorio para dirimirlo.

**Por qué esto importa más que una discrepancia de documentación:** las dos
formas difieren cualitativamente, no solo en constantes.

| b (SoC) | Psi = 1-b^2 (FSD) | Psi por tramos (código) |
|---|---|---|
| 0.976 | 0.0474 | **0.0000** |
| 0.967 | 0.0649 | **0.0000** |
| 0.800 | 0.3600 | **0.0000** |
| 0.500 | 0.7500 | 0.0000 |
| 0.350 | 0.8775 | 0.2500 |
| 0.150 | 0.9775 | 1.0000 |

`1-b^2` **nunca es cero**: en el rango de la campaña principal (b entre 0.967 y
0.976) discrimina entre el nodo más y menos cargado por 0.0175 de Psi, aportando
~0.019 al costo del enlace. La forma por tramos aporta **exactamente 0.000**.

**Consecuencia:** el hallazgo "el aporte energético nunca se activa" es válido
para la fórmula **implementada**, no para la **documentada**. Con `Psi=1-b^2` el
término habría estado activo en las 360 corridas de la campaña principal, y sus
resultados habrían sido otros.

**Decisión requerida (de los autores, no del simulador):** establecer cuál de las
dos formas defiende la tesis y alinear código y documento. Si es la del FSD,
todas las campañas de hoy —incluidas las de vida útil y densidad— midieron una
métrica que la tesis no defiende. Si es la del código, el FSD está obsoleto desde
febrero y debe corregirse.

## 2026-07-20 — Grupo 2: F1.5 como TestSuite y F2.4 cerrado por atribución

**F1.5 Bellman-Ford promovido a TestSuite** (`dv-cl-convergence`). El script
offline `bf_check.py` post-procesaba CSVs; la suite ahora **conduce el routing
real**: una red sintética de instancias `DvClRouting` intercambia anuncios por
`GetBestRoutes`/`UpdateFromDvMsg`, las mismas llamadas que hace la aplicación, de
modo que no se reimplementa nada de la composición de costos. Sobre una grilla
3x3 verifica las tres propiedades que un DV debe cumplir: **completitud** (todo
par conectado tiene ruta), **ausencia de bucles** (seguir next-hops termina) y
**optimalidad** (ningún camino excede el de Bellman-Ford, calculado aparte). Un
segundo caso cubre lo que el conteo de saltos no puede: un desvío de dos saltos
rápidos debe ganarle a un enlace directo lento.

Dos aprendizajes quedaron codificados en la suite:

- Cada `DvClRouting` **debe** recibir `SetNodeId`; con el valor por defecto todas
  las instancias se creen el nodo 0 y rechazan rutas a ese destino como si fueran
  a sí mismas. (Era un fallo de mi arnés, no del producto.)
- El margen de costo del desvío **debe superar la histéresis de conmutación**: la
  ruta directa se aprende una ronda antes y el routing se niega, correctamente, a
  conmutar por una mejora marginal.

**F2.4 conservación: residual atribuido, no solo acotado.** Se instrumentaron los
descartes por desbordamiento de cola (dos sitios que faltaban) y se reporta como
destino final lo que queda encolado al detenerse la aplicación. El test además
**atribuye** el residual distinguiendo tramas que llegaron al aire de las que no.

Resultado en la corrida de referencia (4 nodos, duty 1%, 500 s):

| | antes | ahora |
|---|---|---|
| generados | 36 | 36 |
| entregados | 11 | 11 |
| terminados | 1 | **24** |
| sin explicar | 24 | **1** (perdido en el aire) |

**Hallazgo de método (ns-3), RESUELTO en el código:** `Simulator::Stop` en el
**mismo instante** que el `SetStopTime` de las aplicaciones puede terminar la
simulación **antes** de que `StopApplication` se ejecute, perdiéndose toda la
contabilidad de cierre. Era la causa de que 23 tramas encoladas no aparecieran.

En vez de dejarlo como regla a recordar —que cada escenario nuevo tendría que
redescubrir— se **eliminó el modo de fallo**: `DvClApp::DoDispose()` ancla el
reporte a la destrucción, que siempre ocurre, y el guardia de flush preexistente
lo hace idempotente. El test de conservación ahora **para deliberadamente en el
instante adverso** y sigue cuadrando, de modo que la trampa quedó convertida en
algo que la suite detectaría en lugar de algo que un comentario pide evitar.

El único residual restante es una trama transmitida que nunca llegó: **la pérdida
en el canal no produce evento terminal a nivel de aplicación en ninguno de los
dos extremos**, y es correcto que así sea. Por eso el test reporta y atribuye el
residual en lugar de exigir cero.

## 2026-07-20 — F2.3: la captura explica el piso (aislada por medición)

Habilitado `--collisionMatrix` y repetido el barrido con **ambas** matrices. Para
que la matriz surta efecto hacen falta **dos** flags, no uno: el modelo
`pueyo_fixed_capture` que fuerza el perfil resuelve las colisiones por su propia
vía (`lora-interference-helper.cc:363`) y **nunca consulta la matriz**, que solo
se lee bajo `GOURSAUD_PROBABILISTIC` (línea 493). La primera tentativa —solo
`--collisionMatrix`— dio resultados idénticos byte a byte, por eso.

| G | S con matriz ALOHA | S con matriz Goursaud |
|---|---|---|
| 0.07 | 0.0383 | 0.0380 |
| 0.25 | 0.0507 | 0.0664 |
| 0.65 | 0.0277 | 0.0531 |
| 1.43 | 0.0211 | 0.0555 |
| 2.29 | **0.0037** | 0.0385 |
| 3.19 | **0.0027** | 0.0290 |

**Hipótesis confirmada:** con colisiones totalmente destructivas (matriz ALOHA)
la curva **decae hacia cero** al saturar, mientras que con Goursaud **se aplana
en ~0.03**. El piso que se observaba antes **es el efecto captura**, ahora
demostrado en vez de supuesto. Datos en
`tools/validation/aloha_matrix_comparison.csv`.

**Ajuste cuantitativo: CERRADO** (suite `dv-cl-aloha`). Los dos supuestos que
faltaban por levantar —tráfico Poisson y un único SF— no se pueden satisfacer a
través de la aplicación, cuyo planificador no es Poisson y usa dos SF. Se
resolvió **ejercitando el modelo de interferencia directamente**, que es lo que
el ancla realmente valida: llegadas exponenciales de duración fija, un solo SF,
igual potencia, matriz ALOHA. Cada trama se registra **en su propio instante**
mediante eventos agendados, de modo que el modelo ve el patrón real de solapes.

Resultado: **S concuerda con G·e^(−2G) dentro del 15%** en G = 0.1, 0.25, 0.5,
1.0 y 2.0. El test no puede pasar de forma vacua: si el modelo no destruyera
nada, el cociente sería e^(2G) ≈ 2.7 en el pico.

**F2.3 VALIDADO**, cualitativa y cuantitativamente.

**Recomendación de uso:** `goursaud` (con captura) es el modelo realista y debe
seguir siendo el de las campañas — es el comportamiento del hardware LoRa. La
matriz `aloha` es un **instrumento de validación**, no un escenario a reportar:
existe para contrastar contra la teoría y solo para eso.

## 2026-07-20 — (superado por la entrada anterior) F2.3 primer intento

Escenario de libro de texto: 30 m de separación (un solo dominio de colisión),
todos los nodos hacia el nodo 0, un salto, perfil ALOHA (sin CSMA), sin duty. La
carga ofrecida se barre aumentando el número de nodos. G = aire ocupado /
ventana; S = entregas x ToA medio / ventana. Datos en
`tools/validation/aloha_offered_load_sweep.csv`.

| nEd | G | S medido | S = G·e^(−2G) | ratio |
|---|---|---|---|---|
| 9 | 0.068 | 0.0489 | 0.0593 | 0.82 |
| 25 | 0.256 | 0.0770 | 0.1533 | 0.50 |
| 49 | 0.667 | 0.0548 | 0.1757 | 0.31 |
| 81 | 1.478 | 0.0548 | 0.0769 | 0.71 |
| 121 | 2.315 | 0.0320 | 0.0226 | 1.42 |
| 169 | 3.234 | 0.0326 | 0.0050 | 6.5 |
| 225 | 4.297 | 0.0315 | 0.0008 | 39 |

**Lo que sí se reproduce (cualitativo):** el colapso por contención. S crece,
alcanza un máximo y decae al aumentar la carga — el comportamiento característico
de un acceso aleatorio sin coordinación.

**Lo que NO se reproduce (cuantitativo):** ni la posición del máximo (medido
G≈0.26, teórico G=0.5), ni su valor (0.077 frente a 0.184 = 1/2e), ni el
decaimiento asintótico: el simulador **se aplana en S≈0.03** mientras la teoría
tiende a cero.

**Explicaciones estructurales plausibles**, ninguna aislada todavía: el PHY
modelado tiene **efecto captura** (una señal fuerte sobrevive la colisión, lo que
produce exactamente un piso como el observado), el tráfico es **programado y no
Poisson** (la teoría asume llegadas de Poisson), y conviven **dos SF** (7 y 8)
que son cuasi-ortogonales y por tanto no colisionan entre sí.

**El experimento de aislamiento quedó bloqueado**: `--interferenceModel=goursaud`
y `EnableProbabilisticCapture=false` **no tienen efecto** porque el perfil aplica
`applyPueyoComparableBase()` *después* de parsear la línea de comandos y
sobrescribe ambos. La corrida "sin captura" dio resultados idénticos byte a byte
a la corrida con captura, lo que confirma que el flag se ignoró (no que la
captura sea irrelevante).

**Estado: F2.3 NO validado.** Antes de volver a intentarlo hay que decidir si el
ancla es siquiera aplicable: la ALOHA pura supone llegadas de Poisson, un único
canal, colisiones totalmente destructivas y receptores ilimitados; este PHY no
cumple ninguna de las cuatro. Es posible que el ancla correcta para LoRa sea la
variante **con captura** en lugar de la clásica. Para dirimirlo hace falta (a)
permitir que la línea de comandos sobrescriba el modelo de interferencia del
perfil, y (b) una fuente de tráfico Poisson.

## 2026-07-20 — Cuánto del PDR depende de suponer SF ortogonales

**Qué modelo produce realmente los resultados.** El módulo `lorawan` trae por
defecto Goursaud en los dos ejes, pero el perfil de campaña
(`applyPueyoComparableBase`) fuerza `interferenceModel = pueyo_fixed_capture`. Y
como ese modelo **nunca consulta la matriz de colisión**, la matriz Goursaud es
**configuración muerta** en todas las corridas: su valor es indiferente.

Lo que decide las colisiones en los resultados es `PUEYO_FIXED_CAPTURE`, con
tres reglas:

1. **Ignora por completo la interferencia entre SF distintos** (`continue` sin
   evaluar). No es cuasi-ortogonalidad con umbrales como Goursaud (−16 a −36 dB):
   es **ortogonalidad perfecta**.
2. **Captura por umbral fijo de 6 dB** entre tramas del mismo SF.
3. **Colisión por temporización de preámbulo** aunque la potencia alcance.

Al describir el modelo en el paper corresponde citar `pueyo_fixed_capture` con
estas reglas. **Decir "Goursaud" sería incorrecto**: no es el modelo que produjo
los números.

**Impacto medido de la suposición** (barrido pareado por semilla, 10 semillas,
perfil `proposal_pueyo_like_csmacad`, SF7-SF12 en juego, 7200 s):

| nEd | PDR con SF ortogonales | PDR con cross-SF (Goursaud) | delta | t pareado |
|---|---|---|---|---|
| 9 | 0.1823±0.0041 | 0.1835±0.0044 | +0.6% | 0.54 |
| 25 | 0.0340±0.0009 | 0.0292±0.0014 | **−14.2%** | −8.11 |
| 49 | 0.0146±0.0007 | 0.0088±0.0005 | **−39.5%** | −21.56 |

**La suposición infla el rendimiento, y el efecto crece fuerte con la densidad.**
A 9 nodos es indistinguible de cero (t=0.54): con pocos vecinos apenas hay
solapes entre SF distintos. A 49 nodos el PDR cae **casi a la mitad** al modelar
interferencia cross-SF, con t=−21.6 — de los efectos más grandes y significativos
medidos en toda la sesión.

**Implicación:** los resultados de densidad alta descansan de manera sustancial en
que los SF no interfieran entre sí. Es una limitación que conviene declarar
explícitamente, y un revisor puede preguntarlo con razón. La alternativa honesta
es reportar ambos modelos, o justificar la ortogonalidad citando literatura de
LoRa que la respalde para las separaciones de SF en uso.

Datos en `tools/validation/cross_sf_orthogonality.csv`.

## 2026-07-20 — Goursaud pasa a ser el modelo BASE (decisión de Diego)

Aplicado: `interferenceModel = goursaud` en todos los perfiles y como valor por
defecto. `pueyo_fixed_capture` queda disponible como variante de estudio vía
`--allowInterferenceModelOverride=true`. La validación de perfil pasa a exigir
goursaud. Los 8 perfiles corren sin abortar.

**Efecto en el PDR** (9 nodos, 2000 s, semilla 1 — antes y después del cambio):

| perfil | pueyo (SF ortogonales) | **goursaud (base)** |
|---|---|---|
| `proposal_pueyo_like_aloha` | 0.1775 | **0.1084** |
| `proposal_pueyo_like_csmacad` | 0.1741 | **0.1375** |
| `csmacad_free_backoff` | 0.1768 | **0.1308** |
| `pueyo2024_paper_like` | 0.1653 | **0.1159** |
| `pueyo2024_paper_like_csmacad` | 0.1612 | **0.1145** |
| `proposal_pueyo_like` | 0.0339 | **0.0278** |
| `proposal_pueyo_like_observed` | 0.0407 | **0.0366** |
| `pueyo2024` | 0.0379 | **0.0420** |

Nota: a 9 nodos el barrido pareado de 10 semillas no había encontrado efecto
significativo (t=0.54); estas diferencias de una sola semilla son mayores de lo
que ese resultado sugiere y **no deben leerse como el efecto medio** — el efecto
medido y significativo aparece a 25 nodos (−14%) y 49 (−40%).

**⚠ CONSECUENCIA: las tres campañas de hoy quedan obsoletas.**
`campaign_20260720` (360 corridas), `lifetime_20260720` (60) y el barrido de
densidad (40) se produjeron **todas** con `pueyo_fixed_capture`. Sus números no
son comparables con nada generado a partir de ahora. Si se van a reportar, hay
que **recorrerlas con la base nueva**. El costo de cómputo es conocido y bajo (la
campaña completa tardó minutos), de modo que rehacerlas es viable cuando se
decida.

## 2026-07-20 — F2.1 equivalencia con el módulo lorawan: VALIDADA + hallazgo LDRO

Suite `dv-cl-lorawan-equiv`. En un solo salto, lo que el PHY aporta es la
ocupación del canal, de modo que la equivalencia relevante es la del **tiempo al
aire**: conviven dos implementaciones independientes de esa magnitud —`DvClToa`
(verificada contra Semtech AN1200.13) y `LoraPhy::GetOnAirTime` del módulo
estándar—, y un protocolo que presupuesta con una mientras la radio gasta la otra
mal-cotiza cada transmisión.

**Resultado: coinciden dentro de 1 µs en las 288 combinaciones** de la rejilla
(SF7-SF12 x 6 payloads x 4 CR x 2 preámbulos), **siempre que se les pasen los
mismos parámetros**.

**Hallazgo: no se les pasan los mismos parámetros.** Existe una **tercera**
implementación —`DvClApp::ComputeLoRaToAUs`, inline en la aplicación— que asume
`m_de = true`, es decir **LDRO activado**, tal como la especificación LoRa exige
en SF11/SF12 a 125 kHz. Pero `DvClLoraNetDevice::BuildTxParams` fija
`lowDataRateOptimizationEnabled = false` **en todos los SF**.

Consecuencia: **el plano de control presupuesta según la norma y la radio
transmite fuera de norma.** Es el origen de la discrepancia observada al
instrumentar el duty (1.9087 s presupuestados frente a 1.7449 s reales en SF12,
~9%). El sentido del error importa: la compuerta **se cobra de más** a sí misma,
y la métrica **sobrevalora** los SF altos.

El desajuste no es uniforme: depende del `ceil` de la fórmula, de modo que para
algunos payloads el hueco es exactamente cero y para otros llega a ~9%. La suite
recorre payloads 1-100 en SF11 y SF12, verifica que LDRO **nunca acorta** una
trama y acota el hueco máximo, de modo que el día que el device respete LDRO el
test lo advierte en lugar de pasar en silencio.

**Pendiente de decisión (cambia resultados):** hacer que el device honre LDRO en
SF11/SF12 alinearía la radio con la especificación y con el presupuesto. Ya está
declarado como limitación conocida en el README y el RST del módulo.

## 2026-07-20 — Fuente única de tiempo al aire + radio alineada con la norma

Cerradas de una vez las dos cosas: la duplicación y la desalineación, que eran
la misma causa.

**Antes había tres implementaciones** del tiempo al aire: `DvClToa` (validada
contra Semtech AN1200.13), `LoraPhy::GetOnAirTime` del módulo estándar, y una
**tercera copia inline en la aplicación** (`DvClApp::ComputeLoRaToAUs`). Las
fórmulas coincidían; los **parámetros no**: la aplicación asumía LDRO activado
—como manda la especificación por encima de 16 ms de tiempo de símbolo— mientras
`DvClLoraNetDevice::BuildTxParams` lo fijaba en `false` para **todos** los SF.

**Ahora:**

- La regla vive en un solo sitio, `LowDataRateOptimizationRequired(sf, bw)`, y
  **ambos lados la consultan**.
- La copia inline de la fórmula **se eliminó**; la aplicación delega en la
  validada.
- El flag `m_de` propio de la aplicación **se retiró** para que nadie reintroduzca
  una segunda decisión.
- **El device honra LDRO en SF11/SF12** (decisión de Diego): las tramas ocupan el
  aire tanto como lo haría una radio real.

Verificado por grep: **una sola** implementación de la fórmula
(`dv-cl-toa.cc`) y **una sola** decisión de LDRO en todo el módulo.

**Esto alarga las transmisiones en SF alto, de modo que los resultados se
mueven** — en la dirección correcta: antes la radio emitía más corto que
cualquier radio real y la compuerta de duty se cobraba de más a sí misma.

### Patrón recurrente de la sesión

Cuatro defectos independientes resultaron ser el mismo: **múltiples fuentes de
verdad para una magnitud física**.

| magnitud | fuentes | resolución |
|---|---|---|
| formato del beacon | 2 clases byte-idénticas + rebuild en TX | un header, el que se parsea es el que se emite |
| tiempo al aire | 3 implementaciones, 2 reglas de LDRO | una fórmula, una regla, el device es el dueño |
| carga de batería | registro + capacidad paralela en la app | el registro es el dueño |
| presupuesto de duty | ToA del tag vs ToA de la PHY | la compuerta pregunta al device |

En los cuatro casos la corrección **no fue documentar la sutileza sino eliminar
la posibilidad de divergencia**. Conviene tenerlo como criterio de revisión: si
una magnitud física se calcula en dos sitios, es cuestión de tiempo que
discrepen, y el síntoma es silencioso.

## 2026-07-20 — RE-CORRIDA sobre la base nueva (Goursaud + LDRO): 280 corridas

Las tres campañas rehechas con el simulador corregido. **280/280 sin fallo.**
Índice en `tools/validation/rerun_20260720_index.csv`.

**PDR medio ± desv (10 semillas), campaña principal:**

| perfil | n=9 | n=25 | n=49 |
|---|---|---|---|
| `proposal_pueyo_like_csmacad` | **0.1825±0.006** | **0.0286±0.001** | **0.0090±0.001** |
| `pueyo2024_paper_like_csmacad` | 0.1768±0.007 | 0.0268±0.001 | 0.0079±0.000 |
| `csmacad_free_backoff` | 0.1690±0.009 | 0.0262±0.001 | 0.0079±0.001 |
| `proposal_pueyo_like_aloha` | 0.0780±0.035 | 0.0107±0.001 | 0.0028±0.001 |
| `pueyo2024_paper_like` | 0.0758±0.034 | 0.0095±0.002 | 0.0023±0.001 |
| `proposal_pueyo_like` (duty 1%) | 0.0238±0.001 | 0.0095±0.001 | 0.0033±0.000 |

**Los perfiles ahora SE SEPARAN.** Bajo el modelo anterior los cinco comparables
eran indistinguibles; al modelar la interferencia cross-SF aparece un orden
estable y la propuesta queda primera en las tres escalas.

**Comparación composite_score vs toa_only** (pareada por semilla, ambos con
CSMA/CAD):

| campaña | nEd | dPDR | t | dFND | t | dT50 | t |
|---|---|---|---|---|---|---|---|
| principal | 9 | +3.3% | 1.90 | — | | — | |
| principal | 25 | +7.0% | **6.55** | — | | — | |
| principal | 49 | +14.2% | **5.20** | — | | — | |
| vida útil | 25 | +7.1% | **4.72** | +7.1% | **3.67** | +4.3% | **4.93** |
| vida útil | 49 | +11.8% | **16.61** | +4.5% | **3.06** | +2.4% | **2.39** |
| vida útil | 81 | +16.1% | **11.64** | +8.2% | **3.54** | +5.2% | **9.93** |
| vida útil | 100 | +17.3% | **14.70** | +9.8% | **5.15** | +5.6% | **11.99** |

**REVERSIÓN del resultado anterior.** Con el modelo de SF ortogonales la métrica
compuesta **costaba** PDR (−0.3% a −5.7%). Con interferencia cross-SF real
**lo mejora** (+2% a +17.3%), y a la vez alarga la vida de la red. Ya no es un
compromiso: mejora ambos ejes, y la ventaja **crece con la densidad**.

**Precisión necesaria sobre qué aísla cada comparación:**

- `routeMetricMode` conmuta **toda la función de costo**, no solo el término
  energético: `TOA_ONLY` usa `ToaHopCostUnits`, una formulación distinta de
  `α·ToA + β + δ·Ψ`. La comparación mide **la métrica compuesta como un todo**
  frente a la de solo-ToA.
- En la campaña principal la carga mínima es **0.47-0.49**, apenas bajo `b_hi`:
  Ψ ≈ 0.007, que por δ=0.25 aporta ~0.002 al costo, **despreciable** frente a
  α·ToA (hasta 0.60). Ese +14.2% **no es atribuible al término energético**.
- En la campaña de vida útil la carga llega a 0 y FND está definido, de modo que
  ahí Ψ **sí** actúa plenamente — pero la comparación sigue mezclando
  formulación y energía.

**Aislamiento pendiente (barato):** para atribuir el efecto al término energético
hace falta comparar `composite` contra `composite con δ=0` —misma formulación,
solo cambia Ψ— vía `--ns3::dvcl::DvClCompositeMetric::WEnergy=0`. Es el
experimento que respalda la afirmación central de la tesis.

## 2026-07-20 — AISLAMIENTO DEL TÉRMINO ENERGÉTICO: sin efecto medible

Experimento estricto: **misma formulación, misma configuración, misma semilla;
lo único que cambia es delta** (`--compositeWEnergy=0.25` frente a `0`). Es la
única comparación que atribuye un efecto a `delta*Psi` — la de
`composite_score` contra `toa_only` conmuta la función de costo entera y por
tanto **no** aísla nada.

**Todos-contra-todos** (100 corridas, 10 semillas por punto):

| nEd | dPDR | t | dFND | t | dT50 | t |
|---|---|---|---|---|---|---|
| 9 | −0.1% | −0.08 | −2.5% | −0.41 | +4.6% | 1.23 |
| 25 | +2.0% | 1.44 | +2.3% | 1.41 | +1.7% | 2.17 |
| 49 | +0.2% | 0.23 | −1.6% | −1.16 | −0.7% | −0.85 |

**Por patrón de tráfico** (120 corridas; hipótesis de Diego: la carga uniforme de
todos-contra-todos no genera la heterogeneidad de batería que Psi necesita, y un
sumidero debería generarla):

| topología | nEd | dPDR | t | dFND | t | dT50 | t |
|---|---|---|---|---|---|---|---|
| todos-a-todos | 25 | +2.0% | 1.44 | +2.3% | 1.41 | +1.7% | 2.17 |
| todos-a-todos | 49 | +0.2% | 0.23 | −1.6% | −1.16 | −0.7% | −0.85 |
| multi-sink (4) | 25 | +1.6% | 0.32 | +0.5% | 1.00 | −0.1% | −0.40 |
| multi-sink (4) | 49 | +4.9% | 1.29 | +0.0% | 0.03 | −0.5% | −1.25 |
| sumidero único | 25 | +3.5% | 1.01 | +0.6% | 0.75 | +0.4% | 1.16 |
| sumidero único | 49 | −3.7% | −0.63 | +0.2% | 0.42 | +0.0% | 0.10 |

**Resultado: el término energético no tiene efecto medible en ninguna topología.**
Los 18 contrastes tienen |t| < 2.2, la mayoría bajo 1.3, y los signos se invierten
entre tamaños y patrones. No es que el efecto sea pequeño: es indistinguible de
cero.

Los flags de topología sí actuaron (a2a genera 60 000 paquetes contra 2 400 del
sumidero único, y la red vive 6x más), de modo que el resultado no es un
artefacto de configuración.

**La hipótesis de la heterogeneidad no se sostiene con estos datos.** El proxy
disponible —el cociente T50/FND, que mide cuán dispersas están las muertes— vale
1.75-1.82 en todos-contra-todos y **1.61-1.71 con sumidero**, es decir el
sumidero produce un desgaste *ligeramente más uniforme*, no más dispar. La
convergencia de tráfico agota a los nodos cercanos al sink, pero también los mata
antes, y el resto se descarga de forma pareja.

**Consecuencia para la tesis.** Las ganancias medidas de `composite_score` sobre
`toa_only` (+7% a +17% de PDR, +5% a +10% de FND) son reales y significativas,
pero provienen de la **formulación del costo basada en ToA**, no del término
energético. La afirmación defendible es sobre la métrica compuesta como
formulación; **atribuirla a `delta*Psi` no está respaldado**.

**Limitación medida, no descartada:** solo se probó con SoC inicial U[15,35%].
Un régimen con heterogeneidad inicial mucho más marcada (p. ej. mitad de los
nodos al 90% y mitad al 20%) podría activar Psi de forma útil. Queda como
pregunta abierta, no como resultado.

Datos: `tools/validation/energy_isolation_index.csv` y
`energy_isolation_topologies.csv`.

## 2026-07-20 — Heterogeneidad máxima: el término energético sigue sin efecto

Última vía para activar `delta*Psi`: carga inicial **bimodal**, mitad de los
nodos al 20% y mitad al 90% (flag nuevo `--socInitBimodal`, determinista por
índice). Con `b_lo=0.20` y `b_hi=0.50` eso pone vecinos con **Psi=1 junto a
vecinos con Psi=0** — el contraste más grande que el término puede recibir.
Mismo aislamiento estricto: delta=0.25 contra delta=0, 10 semillas pareadas.

| topología | nEd | dPDR | t | dFND | t | dT50 | t |
|---|---|---|---|---|---|---|---|
| todos-a-todos | 25 | +1.4% | 2.13 | +2.5% | 2.42 | −0.6% | −0.38 |
| todos-a-todos | 49 | +0.0% | 0.00 | +0.8% | 0.49 | +1.1% | 0.78 |
| sumidero único | 25 | −1.3% | −0.39 | +0.0% | 0.01 | −0.0% | −0.22 |
| sumidero único | 49 | −3.1% | −0.77 | +0.3% | 1.38 | +0.1% | 0.71 |

Dos contrastes rozan t≈2.1-2.4 (todos-a-todos, 25 nodos), pero sobre **12
contrastes** eso es lo esperable por azar, y los signos se invierten con el
sumidero. **No hay efecto sostenido.**

**Diagnóstico mecánico:** entre delta=0 y delta=0.25 el número de rutas
instaladas difiere en **0.02%-1.41%**. Es decir, con el contraste máximo posible,
el término energético **apenas altera las decisiones de encaminamiento**. No es
que cambie las rutas y el resultado no mejore: es que casi no cambia las rutas.

*Salvedad del diagnóstico:* se compara el **número** de rutas, no su identidad.
Dos corridas podrían instalar la misma cantidad eligiendo saltos distintos, así
que esto acota el efecto pero no lo demuestra a nivel de decisión. La
comprobación fuerte —comparar el siguiente salto por (nodo, destino) entre ambas
configuraciones— queda pendiente y es barata.

### Conclusión sobre el aporte energético

Tres experimentos independientes, **300 corridas**, todos negativos:

1. Todos-contra-todos, SoC U[15,35%], 5 tamaños → sin efecto.
2. Tres patrones de tráfico (a2a, multi-sink, sumidero) → sin efecto.
3. Heterogeneidad bimodal máxima, dos patrones → sin efecto.

**`delta*Psi`, tal como está formulado, no influye de manera medible en los
resultados.** Se le dio la señal más fuerte concebible y no la aprovechó, lo que
apunta a la **formulación** y no al escenario.

**Lo que sí queda en pie, y es sólido:** la métrica compuesta supera a la de
solo-ToA en entrega y en vida útil (+7% a +17% de PDR, t hasta 16.6), pero el
mérito es de la **formulación del costo basada en ToA** (`alpha*T_hat + beta`),
no del término energético.

**Siguiente paso natural** (no ejecutado): comprobar por qué el término no
reordena rutas. Hipótesis a contrastar, en orden de coste: el término de ToA
domina el rango de costo (0-0.60 frente a 0-0.25); la penalización se aplica al
siguiente salto pero se diluye al acumular costo de camino sobre varios saltos;
o la histéresis de conmutación absorbe la diferencia. Ninguna está verificada.

Datos: `tools/validation/energy_isolation_bimodal.csv`.

## 2026-07-20 — POR QUÉ el término energético no cambia nada: diagnóstico

Investigación del resultado nulo. Se descartaron tres hipótesis y se encontró la
causa.

**Hipótesis 1 — dilución del costo de camino: DESCARTADA (por código).** En
`UpdateFromDvMsg` el costo acumula `pathRaw + ComputeThesisLinkCost(...)`, donde
`pathRaw` es el costo anunciado por el vecino y ya contiene la penalización
energética de *sus* saltos. La energía **se suma hop a hop** igual que el resto;
no se diluye.

**Hipótesis 2 — saturación del encoding: DESCARTADA (por código).** COST255 con
paso 0.025 representa hasta 6.375 de costo de camino, muy por encima de lo que
alcanza un camino típico (~0.3-0.5 por salto). Y `delta*Psi` máximo (0.25) son
**10 pasos de cuantización**: resolución de sobra.

**Hipótesis 3 — el término no llega a la decisión: DESCARTADA (por test).**
Nueva suite `dv-cl energy: the fuller relay wins an otherwise equal tie`: dos
relevos a igual distancia, igual ToA e igual número de saltos, uno vacío
(3000 mV) y otro lleno (4200 mV). **El routing elige al cargado.** El test
incluye el control inverso —intercambiar las cargas invierte la elección— de
modo que no pasa por un desempate de id. **El mecanismo funciona.**

**CAUSA REAL: el ruteo solo controla una fracción menor del gasto energético.**

| topología | nEd | balizas TX | datos TX | control/datos | overhead bytes |
|---|---|---|---|---|---|
| todos-a-todos | 25 | 124 941 | 41 137 | **3.0** | 7.9 |
| todos-a-todos | 49 | 244 766 | 137 138 | **1.8** | 8.7 |
| sumidero único | 25 | 124 997 | 1 774 | **70.5** | 182.7 |
| sumidero único | 49 | 244 976 | 3 216 | **76.2** | 375.1 |

Con sumidero hay **70-76 balizas por cada transmisión de datos**. Las balizas son
broadcast periódico: **cada nodo las emite pase lo que pase**, con independencia
de las rutas que cualquiera elija. El encaminamiento solo decide quién releva
datos, que es la porción pequeña del consumo.

Por eso esquivar a un nodo descargado no le alarga la vida de forma apreciable:
se le quita una fracción menor de su gasto mientras sigue pagando el plano de
control completo. **El término energético hace exactamente lo que debe, sobre la
variable equivocada.**

### Consecuencia de diseño (para la tesis)

Esto convierte el resultado nulo en un hallazgo con dirección. Un ruteo consciente
de energía **solo puede rendir cuando el relevo de datos domina el gasto**. Con la
cadencia de balizas actual eso no ocurre en ningún régimen probado.

Vías coherentes con el diagnóstico, en orden de coste:

1. **Reducir la cadencia de balizas** (o hacerla adaptativa) hasta que el relevo
   sea una fracción apreciable del consumo, y repetir el aislamiento. Es la
   prueba directa de este diagnóstico.
2. Que el término energético gobierne también el **plano de control** —por
   ejemplo, espaciar las balizas de los nodos con poca carga—, que es donde
   está el gasto.
3. Reportar el hallazgo tal cual: en redes LoRa con este régimen de control, la
   vida útil la fija el overhead de balizas y no las decisiones de ruteo. Es un
   resultado negativo **informativo** y publicable.

## 2026-07-20 — Prueba del diagnóstico: REFUTADO (el overhead no era la causa)

El diagnóstico anterior sostenía que `delta*Psi` no rinde porque el ruteo
gobierna solo una fracción menor del gasto —70-76 balizas por dato— y que
bajando la cadencia el término empezaría a actuar. **La predicción falla.**

Barrido de cadencia con aislamiento de delta en cada nivel (96 corridas, 8
semillas). Advertencia de Diego atendida: el vencimiento de rutas escala solo
(`routeTimeout = intervalo * factor 5`), verificado además empíricamente.

**El plano de control sigue sano al bajar la cadencia**, y el peso del relevo se
invierte como se buscaba:

| beacon | nEd | rutas | PDR | control/datos |
|---|---|---|---|---|
| 60 s | 25 | 1144 | 0.0282 | **3.04** |
| 240 s | 25 | 1120 | 0.0301 | **0.76** |
| 900 s | 25 | 1123 | 0.0417 | **0.24** |
| 60 s | 49 | 4508 | 0.0087 | 1.78 |
| 240 s | 49 | 4391 | 0.0092 | 0.44 |
| 900 s | 49 | 4396 | 0.0127 | **0.13** |

A 900 s el relevo de datos supera al control **8 a 1** (ratio 0.13), justo la
condición que el diagnóstico exigía. Las rutas se mantienen (1123 frente a 1144)
y el PDR **mejora** (0.0417 frente a 0.0282), de modo que el régimen es válido y
no un colapso.

**Y el término energético sigue sin actuar:**

| beacon | nEd | dPDR | t | dFND | t | dT50 | t |
|---|---|---|---|---|---|---|---|
| 60 s | 25 | +3.2% | 2.28 | +1.6% | 0.88 | +1.7% | 2.25 |
| 60 s | 49 | −0.0% | 0.00 | −1.3% | −0.98 | −1.2% | −1.40 |
| 240 s | 25 | +3.5% | 3.57 | +0.6% | 0.22 | +2.9% | 1.61 |
| 240 s | 49 | −1.5% | −1.67 | +4.1% | 1.76 | −2.1% | −2.06 |
| 900 s | 25 | +4.8% | 1.35 | +3.1% | 0.99 | +1.2% | 0.52 |
| 900 s | 49 | +0.6% | 0.33 | +2.9% | 0.75 | −1.4% | −1.09 |

Sobre 18 contrastes, uno alcanza t=3.57 (PDR a 240 s, 25 nodos) y el resto queda
bajo 2.3, con signos que se invierten entre tamaños. **No hay tendencia con la
cadencia**: si el diagnóstico fuera correcto, el efecto debería crecer
monótonamente de 60 s a 900 s en FND y T50, y no lo hace.

**Estado: la causa del resultado nulo sigue sin identificarse.** Lo establecido
con datos:

- El mecanismo **funciona** a nivel de decisión (test de preferencia con control
  inverso).
- **No** es dilución de costo de camino ni saturación del encoding (por código).
- **No** es la topología de tráfico (3 patrones), ni la falta de heterogeneidad
  (reparto bimodal 20/90), ni el peso del overhead de control (esta entrada).

Hipótesis vivas, ninguna probada: que la vida útil esté dominada por el consumo
en recepción y escucha —que ningún esquema de ruteo redistribuye, porque todo
nodo escucha igual—, o que evitar a un nodo como relevo no reduzca su consumo lo
suficiente frente a lo que gasta transmitiendo lo suyo propio. La primera es
comprobable con el desglose por estado del `DvClLoraEnergyModel`, que ya registra
tiempo por estado.

Datos: `tools/validation/beacon_cadence_sweep.csv`.

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

## 2026-07-22 — Por qué el término de energía no rinde: el techo es 1.4%

Desglose del gasto por actividad, leído del registro `DvClEnergyRegistry` —el
libro que gobierna el SoC anunciado en la baliza, el argumento de Psi y la
muerte del nodo. Perfil `proposal_pueyo_like_csmacad`, 25 nodos, 300 ks,
`composite_score`.

| actividad | mAh | % |
|---|---:|---:|
| idle (radio encendida, sin actividad) | 3333.3 | **69.23** |
| RX (recepción) | 375.4 | 7.80 |
| CAD (escucha de portadora) | 5.8 | 0.12 |
| TX balizas | 753.0 | 15.64 |
| TX datos propios | 278.5 | 5.78 |
| **TX datos relevados** | **68.9** | **1.43** |

El reparto del TX por clase de trama sale del airtime real de
`mesh_dv_metrics_tx.csv` (124 997 balizas, 59 766 datos propios, 14 783 datos
relevados; 33 009 s de aire en total).

**La decisión de encaminamiento solo redistribuye el relevo: 1.43% del
presupuesto.** Todo lo demás lo paga cada nodo igual, elija la ruta que elija:
la radio encendida es un piso fijo de 69%, las balizas salen a cadencia fija y
tamaño fijo, y el tráfico propio se emite exista la ruta que exista. Un
reparto *perfecto* de la carga de relevo movería la vida útil ~1.4%, muy por
debajo de la dispersión de FND entre semillas.

Esto cierra la investigación abierta desde el 2026-07-19. `delta*Psi` no está
roto ni mal codificado: se le pide gobernar el 1.4% del gasto. Las cinco
hipótesis previas (dilución del coste, saturación de la codificación, el
término no llega a la decisión, topología, homogeneidad de carga) quedaron
descartadas una a una por medición; ésta es la que las explica a todas.

**Consecuencia para la tesis.** El resultado que sobrevive —`composite_score`
gana a `toa_only` en PDR y en FND— es mérito de la formulación del coste por
tiempo en aire, no del término de energía. Y el hallazgo de fondo es
publicable por sí mismo: *en una malla LoRa de receptores siempre encendidos,
ninguna métrica de ruteo puede alargar la vida de la red de forma apreciable,
porque el ruteo no toca el 98.6% del consumo.* Lo que sí lo alargaría, en
orden de palanca: dormir la radio entre balizas (69%), bajar la cadencia de
balizas (15.6%), y sólo después repartir el relevo (1.4%).

Régimen donde `delta*Psi` tendría margen: aquel en que el relevo domine el TX.
Aquí es el 6.3% del aire porque la rejilla de 25 nodos en todos-contra-todos
resuelve casi todo en un salto. Pendiente: medir la fracción de relevo en las
topologías ya barridas y buscar una donde suba un orden de magnitud, antes de
dar el término por muerto.

### Cuatro defectos encontrados por el camino

1. **La campaña abortaba en build de debug.** `BasicEnergySource` afirma
   `m_remainingEnergyJ >= energyToDecreaseJ`; ns-3 nunca acota su propia carga
   a cero, delega en que el modelo de dispositivo deje de consumir al agotarse
   (así lo hace `WifiRadioEnergyModel`). `DvClLoraEnergyModel` pasaba a SLEEP
   pero seguía tirando 0.2 uA, hundiendo la fuente sin límite. En build
   optimizado el aserto no está y pasaba silencioso. Corregido con un flag
   `m_depleted` que anula el consumo. **Una batería agotada no consume.**
2. **El CAD nunca se cobraba.** El MAC hace sondeo de portadora y jamás avisaba
   al registro: la columna CAD leía cero en todas las corridas. Cableado en
   `PerformChannelAssessment`. Aporta 0.12% —pequeño, pero al piso fijo, no al
   1.4% gobernable.
3. **El estado RX se entraba y no se salía.** `Receive()` ponía RX sin agendar
   el regreso a IDLE (a diferencia de TX, que sí lo hacía). El nodo quedaba en
   RX desde su primera recepción hasta su siguiente transmisión: esa vía
   reportaba 88% de la energía como recepción mientras el libro autoritativo
   decía 7.8%. Corregido cobrando la duración real de la trama.
4. **Dos libros de energía para una misma magnitud** —el patrón de siempre. El
   registro (completo: TX/RX/CAD/idle) gobierna todo lo que se publica; el
   framework ns-3 (`DvClLoraEnergyModel` + `BasicEnergySource`) corre en
   paralelo, con los defectos 1 y 3, y **su traza de agotamiento no la escucha
   nadie**. Los defectos 1 y 3 quedan corregidos, pero la duplicación sigue en
   pie y es una decisión de arquitectura pendiente: o el registro pasa a ser un
   `DeviceEnergyModel` de ns-3, o la vía framework se elimina.

Datos: `tools/validation/energy_budget_breakdown.csv`. Suites: 10/10 PASS.

## 2026-07-22 (b) — El escenario: los datos se acaban en t=24 ks de 300 ks

Buscando un régimen donde el relevo pese más, apareció algo que subsume el
hallazgo anterior. En **toda** corrida de vida útil:

| | |
|---|---|
| último dato propio transmitido | **24 172 s** |
| última baliza | 299 964 s |
| FND | 292 326 s |
| duración de la corrida | 300 000 s |

`PueyoPacketsPerPair=100` da 25 x 24 x 100 = 60 000 paquetes, y se agotan en
los primeros 24 ks. **Los 276 ks restantes —el 92% de la corrida— son balizas
e idle sobre una red sin tráfico.** El primer nodo muere 268 000 s después del
último dato.

Es decir: **el FND que veníamos midiendo es, casi por entero, cuánto aguanta un
nodo balizando en una red vacía.** Ningún esquema de encaminamiento puede
moverlo, porque durante el 92% de la medición no hay nada que encaminar. Esto
explica el 1.43% del desglose anterior: el relevo pesa poco porque los datos
ocupan el 8% de la corrida.

Corregido con `--allowPacketsPerPairOverride`, que permite sostener el tráfico
durante toda la simulación (1400 paq/par cubre 300 ks a la misma cadencia).
Experimento en curso: aislamiento de delta con tráfico sostenido, 4 semillas.

### Cuarta instancia del patrón de flags ignorados en silencio

`applyPueyoComparableBase()` fija `txPowerDbm=20` y `pueyoPacketsPerPair=100`
**después** de parsear la CLI, de modo que `--txPowerDbm` y
`--pueyoPacketsPerPair` se aceptaban y no hacían nada; la guarda posterior
(`NS_ABORT_MSG_IF`) nunca saltaba porque comprobaba el valor ya sobreescrito.
Van cuatro casos iguales (`interferenceModel`, `collisionMatrix`,
`beaconInterval`, y estos dos). Además `--pueyoGridSpacingM` solo actúa en
modos `pueyo_*` y el perfil corría con placement `random`, así que un barrido
de densidad sobre este perfil no varía nada.

**Recomendación de método:** toda corrida debería verificar que los flags
pedidos son los aplicados, comparando la línea de configuración que el binario
imprime contra lo solicitado, en vez de confiar en que el flag se respetó.

## 2026-07-22 (c) — Auditoría del patrón: 34 flags se descartaban en silencio

El patrón apareció cuatro veces (`interferenceModel`, `collisionMatrix`,
`txPowerDbm`, `pueyoPacketsPerPair`), así que en vez de parchear instancias se
auditó entero. `applyPueyoComparableBase()` asigna 40 variables después de
parsear la línea de comandos; 6 tienen override explícito y **34 no**:

```
beaconIntervalStableSec beaconLatestOnly controlBackoffFactor costEncoding
dataBackoffFactor dataFixedPhaseCadence dataPeriodJitterMaxSec
dataPeriodJitterSymmetric dataSlotJitterSec dataSlotPeriodSec
dataStartPhaseMaxSec dataStartPhaseOnly dutyWindowSec dvPayloadMaxBytes
enableDataSlots initTtl maxRoutesPerDestination maxTotalRoutes preambleSymbols
prioritizeBeacons puelloPreambleSymbols pueyoStrictQueueScheduler
routeAdvertPolicy routeSwitchMinDeltaX100 routeTimeoutFactor sfLinkMarginDb
sfMax sfMin sfScanEdThresholdDbm sfScanResetOnNewSignal trafficMode
txPowerDbm useProbabilisticSfForBeacons wireFormat
```

Que el perfil los fije es correcto —es lo que hace comparable la réplica—. Lo
inaceptable es que el binario **acepte el flag y lo descarte sin avisar**: la
corrida dice medir una cosa y mide otra. Ya costó cuatro experimentos, y en el
barrido de topologías de hoy dos filas (`sparse300`, `tx8dbm`) salieron byte a
byte idénticas a la base sin que nada lo señalara.

**Corregido de forma general.** El binario ahora compara lo que se pidió por
línea de comandos contra lo que quedó tras aplicar el perfil, y aborta
nombrando cada flag descartado con el valor pedido y el aplicado:

```
El perfil 'proposal_pueyo_like_csmacad' descarta 2 flag(s) que pediste:
  --initTtl (pedido 5, aplicado 63), --sfMax (pedido 10, aplicado 8).
```

Verificado en cinco casos: un flag pisado aborta; dos pisados se listan ambos;
un flag pasado con el mismo valor que fija el perfil no aborta; un flag con
override explícito disponible no aborta; una corrida normal no aborta.

Nota lateral que el propio guard destapó: pidiendo `--sfMax=10` el valor
aplicado es 8, no el 12 que fija `applyPueyoComparableBase()`. Hay un ajuste
posterior específico del perfil. Sin revisar.

### Corrección del recuento, y la rutina que faltaba

El recuento anterior (34) estaba mal: la auditoría solo reconocía overrides de
una asignación, y varios son bloques de dos. El número correcto es **9 con
override y 31 sin**:

| flag | override |
|---|---|
| `beaconIntervalWarmSec`, `beaconIntervalStableSec` | `--allowBeaconOverride` |
| `sfMin`, `sfMax` | `--allowPaperLikeSfRangeVariant` |
| `interferenceModel` | `--allowInterferenceModelOverride` |
| `pueyoPacketsPerPair` | `--allowPacketsPerPairOverride` |
| `dataPayloadSizeBytes` | `--allowPayloadOverride` |
| `shadowingSigmaDb` | `--allowShadowOverride` |
| `enableNs3EnergyFramework` | `--allowEnergyFwOverride` |

El mensaje de error ahora nombra el override cuando existe y dice que no lo hay
cuando no:

```
descarta 2 flag(s) que pediste:
  --initTtl (pedido 5, aplicado 63) -> este parametro no admite override;
  --sfMax  (pedido 10, aplicado 8)  -> usa --allowPaperLikeSfRangeVariant=true
```

Esto cierra también el cabo suelto de `--sfMax=10 -> 8`: no era un defecto, es
el rango Pueyo (SF7-8) que el perfil fija a propósito, y tiene override.

**`tools/validation/preflight.sh`.** El aserto de `BasicEnergySource` llevaba
meses ahí y se encontró por accidente: las campañas corren en un árbol
optimizado, donde `NS_ASSERT` no existe, y nadie pasaba nunca por un árbol con
asertos. La rutina corre con asertos activos y toca en segundos los caminos que
una campaña larga acaba tocando:

1. build del módulo y del ejemplo de campaña;
2. las 10 suites;
3. arranque de los 9 perfiles;
4. **agotamiento de batería** — arrancando con SoC al 2-3% los nodos mueren en
   segundos en vez de en 300 ks, y la rutina falla si *nadie* muere (si no, da
   falsa tranquilidad sin recorrer el camino);
5. que un flag pisado por el perfil aborte en vez de colarse.

Estado actual: **PREFLIGHT OK**, con `fnd_s=5185` en la etapa 4, es decir el
camino de agotamiento sí se recorre.

## 2026-07-22 (d) — Con tráfico sostenido, el término de energía SÍ rinde

Aislamiento de `delta` con tráfico durante toda la corrida (1400 paq/par, 300
ks, 25 nodos, `proposal_pueyo_like_csmacad`), 8 semillas, pareado por semilla:

| magnitud | delta=0.25 vs delta=0 | t | semillas |
|---|---:|---:|---|
| **FND** | **+9.69%** | **17.49** | 8/8 a favor |
| PDR | −0.78% | −3.97 | 8/8 en contra |
| relevo (% del TX) | −1.06 pp | −24.13 | 8/8 |
| energía TX total | −0.44% | −14.24 | 8/8 |

**El término compra ~9.7% de vida útil a cambio de ~0.8% de PDR.** Es una
frontera de Pareto real, y es exactamente el encuadre de "división de labores".

El resultado nulo de los cinco experimentos anteriores no era del término: era
del escenario. Sin tráfico durante el 92% de la corrida no había nada que
repartir.

Datos: `tools/validation/sustained_traffic_delta_isolation.csv`.

### Aviso de método: un binario reconstruido a mitad de lote

El primer lote cruzó una recompilación (el arreglo del cierre del libro), y se
ve en `idle_mah` entre la semilla 5 `d=0.25` (2638) y `d=0` (1952). Las
columnas de energía de ese lote no son comparables entre sí: daban −2.35% de
energía TX cuando el valor real, medido con un único binario, es **−0.44%**.
FND, PDR y relevo no se vieron afectados porque no dependen de los contadores.

De rebote salió una comprobación de determinismo que no habíamos hecho: el
relanzado con el binario corregido dio FND y PDR **idénticos dígito a dígito**,
confirmando que el arreglo de contabilidad no toca la dinámica.

**Regla:** no recompilar mientras un lote corre; y todo conjunto que vaya al
paper debe salir de un único binario.

### El mecanismo, todavía sin demostrar

Un recorte del 0.44% en energía TX —que sobre el presupuesto total es ~0.2%—
no puede explicar por sí solo un +9.69% de vida útil: son 45x de amplificación.
Tiene que ser redistribución, ya que el FND lo fija el nodo peor parado y no la
media. Pero la dispersión del gasto TX entre nodos medida al final de la
corrida apenas se mueve (CV t=−1.78, Gini t=−2.07) y el pico del nodo más
cargado no es concluyente (t=−1.48).

Esas medidas están tomadas al final, cuando la red ya se degradó. Experimento
en curso: cortar a 110 ks —por debajo del FND más temprano observado (112.6
ks), con lo que ningún nodo ha muerto— y comparar a igual duración el mínimo y
el percentil 10 del SoC. Si el término protege al nodo peor parado, es ahí
donde debe verse.

## 2026-07-22 (e) — El mecanismo: redistribución de la cola del SoC

Corte a 110 ks (por debajo del FND más temprano, 112.6 ks: ningún nodo ha
muerto, comparación a igual duración). δ=0.25 frente a δ=0, 8 semillas
pareadas:

| magnitud | media | t | signo |
|---|---:|---:|---|
| SoC mínimo (peor nodo) | **+106.6%** | 1.83 | 8/8 + |
| SoC percentil 10 | **+23.98%** | 11.57 | 8/8 + |
| SoC medio | +1.87% | 19.78 | 8/8 + |
| CV del SoC | **−0.06** | −18.38 | 8/8 − |
| TX del nodo más cargado | +0.88% | 0.35 | mixto |
| CV del gasto TX | +0.00 | 0.94 | mixto |

**El término no reduce el consumo, redistribuye la carga restante.** La media
del SoC apenas cambia (+1.87%), pero la cola inferior se levanta: el nodo peor
parado más que duplica su batería y el percentil 10 sube 24%, con la dispersión
comprimiéndose 18σ. El FND lo fija el nodo que muere primero —un estadístico de
cola— así que un efecto del 0.44% en la media de gasto rinde +9.7% de FND. Esa
es la resolución de la aparente amplificación de 45x: no hay amplificación, hay
una media que no se mueve y una cola que sí.

Detalle que cierra el "cómo": el gasto TX **no** se redistribuye de forma
medible (CV del txMah mixto, t=0.94). El mecanismo no es "el relevo pesado
transmite menos" sino que Psi(b_j) desvía tráfico de quien esté más bajo *en
cada instante*. Es un controlador realimentado que apunta al nodo correcto en
cada momento: mueve poca energía en el agregado, pero siempre se la quita a
quien está más cerca de morir. Por eso la cola del SoC se mueve mucho mientras
la dispersión del gasto acumulado no.

Cautela estadística honesta: el SoC mínimo es el estadístico más ruidoso
(t=1.83, no significativo al 5% con n=8), pero su dirección es 8/8 (test de
signos p=0.008) y su versión robusta, el percentil 10, es inequívoca (t=11.57).
La afirmación defendible es sobre la cola, no sobre el mínimo puntual.

Datos: `tools/validation/mechanism_soc_tail.csv`.

## 2026-07-22 (f) — Un solo modelo de energía, idiomático ns-3

Había dos libros para una batería: un registro a medida (`DvClEnergyRegistry`)
que gobernaba el SoC, la métrica y la muerte del nodo, y un `DeviceEnergyModel`
de ns-3 (`DvClLoraEnergyModel`) cuya notificación de agotamiento no la escuchaba
nadie y que, además, doble-contabilizaba la batería. Los dos se fusionan en una
sola clase.

**`DvClLoraEnergyModel`**, ahora:
- es un `energy::DeviceEnergyModel` de ns-3 de verdad, uno por dispositivo;
- lleva el libro de débito event-driven que ya estaba validado (cobra TX, RX y
  CAD por su duración exacta, e idle de forma diferida) como **única** autoridad
  de carga;
- puede acoplarse a un `EnergySource` (del que toma la tensión de alimentación)
  y dispara `HandleEnergyDepletion` en cuanto el libro llega a cero;
- expone `GetEnergyFraction()`, el desglose por actividad (`GetTxMah` …) y
  `GetTimeInState`, sin el parámetro `NodeId` vestigial del registro.

Se eliminan `dv-cl-energy-registry.{h,cc}` y el helper del modelo paralelo. El
flag `enableNs3EnergyFramework` deja de instalar un segundo modelo; se conserva
como la compuerta que los perfiles ya activan para sembrar el SoC inicial
heterogéneo de los KPI de vida útil.

La contabilidad event-driven es deliberada: una máquina de estados alimentada
desde un hook de "recepción completada" no puede representar el intervalo de
recepción, que es exactamente el defecto que hacía que el modelo viejo
reportara el 88% de la energía como RX.

**Determinismo:** el libro de débito es el mismo, así que el resultado debe
salir idéntico. Comprobado contra los valores congelados del experimento de
aislamiento de delta (proposal_pueyo_like_csmacad, 25 nodos, 300 ks):

| corrida | FND congelado | FND nuevo |
|---|---|---|
| s1 δ=0.25 | 134608 | 134608 |
| s1 δ=0.0 | 125008 | 125008 |
| s3 δ=0.25 | 145745 | 145745 |

Bit-idéntico en FND y PDR. Suites 10/10 (el suite de energía se reescribió a la
API única: cierre del libro E=ΣI·V·t, batería vacía no consume, y el modelo se
acopla a un `EnergySource` real tomando su tensión).

## 2026-07-22 (g) — Sensibilidad de pesos: δ es un interruptor, α y β son inertes

Barrido de α/β/δ por CLI (sin tocar los defaults que los tests pinean), tres
líneas por el punto de tesis (0.60, 0.15, 0.25), tráfico sostenido 300 ks, 25
nodos, 4 semillas, pareado por semilla.

**Línea δ, cada punto frente a δ=0:**

| δ | ΔFND | t | ΔPDR | t |
|---|---:|---:|---:|---:|
| 0.10 | +8.99% | 16.95 | −0.90% | −9.30 |
| 0.25 | +9.06% | 16.16 | −0.77% | −5.70 |
| 0.40 | +9.07% | 16.70 | −0.92% | −7.88 |

**Todo el beneficio se captura en δ=0.10 y satura.** Entre δ=0.10, 0.25 y 0.40
no hay diferencia medible (comparados con el punto de tesis, |ΔFND|<0.1%, t<1).
El término de energía es un interruptor, no una perilla: encenderlo compra el
~9% de vida útil; subir su peso no compra más. El valor de tesis (0.25) está
cómodamente en la meseta, no en un filo.

**α y β, frente al punto de tesis (pareado):**

| combo | ΔFND | t | ΔPDR | t |
|---|---:|---:|---:|---:|
| α=0.40 | +0.26% | 4.76 | −0.27% | −1.10 |
| α=0.80 | +0.04% | 0.27 | −0.20% | −1.96 |
| β=0.05 | −0.01% | −0.10 | −0.25% | −1.25 |
| β=0.30 | −0.03% | −0.30 | −0.28% | −1.86 |

**α y β son inertes en este régimen.** Ni el peso del tiempo en aire ni el
coste por salto mueven FND o PDR de forma apreciable (|Δ|<0.3%). El único
rastro es α=0.40 con +0.26% de FND (t=4.8), despreciable frente al 9% de δ.

**Lectura para la tesis.** La frontera de Pareto PDR–FND se reduce a dos puntos
de operación: δ=0 (PDR marginalmente mayor, +0.78%; vida útil −8.3%) y δ>0 (la
meseta). No es una curva continua de compromiso sino un salto entre dos
regímenes. Y responde la pregunta que quedaba abierta sobre qué peso da mayor
PDR: casi ninguno — el PDR es plano frente a los pesos (máximo en δ=0 por un
+0.78%); los pesos gobiernan la vida útil, no el PDR, y la vida útil satura.
El diseño es robusto: no exige sintonizar δ con precisión ni tocar α/β.

Alcance: una topología (rejilla 25 nodos), un patrón de tráfico (todos-contra-
todos sostenido), un punto de operación. La saturación y la inercia son
propiedades de este régimen; otra densidad o carga podría revelar un gradiente.

Datos: `tools/validation/weight_sensitivity.csv`.

## 2026-07-23 — C5 asentado: bajo duty 1% el término de energía no compra vida útil

Aislamiento de δ bajo duty-on 1% (`time_off_air`), mismo diseño que el
experimento de tráfico sostenido: 25 nodos, 300 ks, 1400 paq/par, 8 semillas
pareadas. Contraste con el mismo experimento sin duty:

| magnitud (δ=0.25 vs δ=0) | sin duty | con duty 1% |
|---|---:|---:|
| FND | **+9.69%** (t=17.5) | **+0.04%** (t=2.9) |
| PDR | −0.78% (t=−4.0) | **+0.52%** (t=4.4, 7/8) |
| relevo (% del TX) | −1.06 pp (t=−24) | −0.48 pp (t=−19.5) |
| energía TX | −0.44% (t=−14) | +0.01% (t=1.4) |

Tres lecturas, en orden:

1. **El efecto sobre la vida útil colapsa 240×** (de +9.69% a +0.04%). No es
   que el término deje de actuar: sigue redistribuyendo el ruteo (relevo
   −0.48 pp, t=−19.5). Es que **el duty cycle ya hizo el trabajo**: con el
   airtime capado al 1% por nodo, ningún relay puede sobre-gastarse, y la
   desigualdad de gasto que δ·Ψ explota queda acotada por regulación. El duty
   cycle actúa como ecualizador implícito de energía.
2. **El canal del beneficio cambia de signo.** Sin duty, δ compra vida útil
   pagando PDR (−0.78%). Con duty, no compra vida útil pero **mejora el PDR**
   (+0.52%, 7/8 semillas): desviar tráfico del relay caliente ahora evita
   relays con presupuesto de duty agotado, y eso entrega más paquetes. El mismo
   mecanismo (evitar al nodo sobrecargado) paga en la moneda que el régimen
   deja libre.
3. **El FND absoluto sube ~45%** con duty (187–210 ks vs 125–146 ks): capar el
   TX alarga la vida de todos. El presupuesto energético duty-on queda: idle
   ~48%, TX ~35%, RX ~13%, CAD ~4%. El relevo es ~1.5% del total, mismo orden
   que el techo medido sin duty.

Consecuencia para el paper (C5 del DoE): el valor del ruteo energy-aware es
**dependiente del régimen regulatorio**. Nulo para vida útil bajo EU868 1%
(donde deja un dividendo pequeño de PDR), y +9.7% de FND donde no hay duty
(US915, ISM sin restricción, o despliegues indoor que lo ignoran, como
Udugampola justifica). El barrido DC% (E4) traza la transición y US915 (E4c)
la triangula. Hasta donde revisamos (NotebookLM `Papers_Magister`), nadie ha
reportado el duty cycle en este rol.

Datos: `tools/validation/dutyon_delta_isolation.csv`. Gate G1 del DoE: cerrado.

## 2026-07-25 — E1/E2 completos: el MAC domina, la métrica de ruteo no mueve el PDR

Campaña E1+E2 del DoE en ns3-remote: **2400 celdas, 0 fallos, 0 huecos**
(3 escenarios × 8 N {9..100} × 20 semillas × {composite, toa, hops} en CSMA/CAD
y {composite, toa} en ALOHA). Régimen principal: duty-on 1%, carga Pueyo, 40 ks.
Todo pareado por semilla.

### Verificación previa: las métricas sí cambian las corridas

Antes de interpretar un resultado nulo hay que descartar que las tres métricas
sean la misma corrida. No lo son: `composite` vs `toa` coinciden exactamente en
solo 7/480 celdas (1%), y la diferencia por celda tiene **mediana 2.17%, p90
8.0% y máximo 20.8%**. La métrica altera el encaminamiento de forma sustancial
en cada corrida; lo que ocurre es que el signo es aleatorio.

### E1 — ranking de métricas (CSMA/CAD, PDR)

| comparación | media | t | a favor |
|---|---:|---:|---|
| composite vs toa | −0.10% | −0.45 | 248/480 |
| composite vs hops | +0.08% | +0.51 | 201/480 |
| toa vs hops | +0.34% | +1.58 | 230/480 |

**Las tres métricas son estadísticamente indistinguibles en PDR.** Con n=480 y
efectos previos detectados a t=16, la potencia sobra: esto es un nulo medido, no
falta de datos.

Hay estructura por escenario, pequeña y de signo cambiante:

| escenario | composite vs toa | t |
|---|---:|---:|
| grid × all-to-all | **+0.75%** | +4.89 |
| random × convergecast | −0.39% | −0.72 |
| random × multisink-4 | −0.65% | −2.07 |

La compuesta ayuda donde hay malla real que explotar (all-to-all) y estorba
levemente donde el destino es único o casi (multisink). Por N no hay tendencia
monótona.

### E2 — interacción MAC × ruteo

| efecto | media | t | a favor |
|---|---:|---:|---|
| **MAC: CSMA/CAD vs ALOHA (composite)** | **+9.28%** | **13.48** | 395/480 |
| **MAC: CSMA/CAD vs ALOHA (toa)** | **+9.40%** | **13.67** | 395/480 |
| métrica bajo ALOHA (composite vs toa) | −0.00% | −0.00 | 239/480 |
| **sinergia** (comp−toa\|CSMA) − (comp−toa\|ALOHA) | −0.10 pp | −0.35 | 228/480 |

**El efecto del MAC es grande y robusto (+9.3% de PDR, 395/480 semillas); el de
la métrica es nulo; y no hay interacción medible entre ambos.** La hipótesis Q4
(sinergia cross-layer MAC×ruteo) **no se sostiene en PDR** con estos datos.

### Lectura preliminar (sujeta a E3)

Con lo medido hasta ahora, y solo en PDR: bajo el duty cycle de EU868 la
elección de MAC gobierna la entrega y la métrica de ruteo casi no. Encaja con
Q5 (2026-07-23): el 1% regulatorio acota tanto el margen de maniobra que el
plano de ruteo tiene poco que optimizar.

**Esto no cierra Q1.** El valor reivindicado de la métrica compuesta es la
**vida útil**, no el PDR, y eso es el bloque E3, aún sin correr. La comparación
de aquí (composite vs toa) cambia la función de coste entera, distinta del
aislamiento de δ (2026-07-23), que sí dio +0.52% de PDR bajo duty. Ambos
resultados conviven: δ dentro de la compuesta ayuda un poco; la compuesta
entera frente a ToA no se distingue.

Observación de régimen: en grid all-to-all el PDR cae a 0.008 en N=100 — la red
está saturada y ninguna métrica rescata eso. Los escenarios convergecast y
multisink degradan mucho más suave (0.067 y 0.130 a N=100).

Datos: `tools/validation/e1e2_results.csv` (2400 filas).

## 2026-07-25 (b) — Métrica RSSI cableada, y una regla de método sobre árboles

`routeMetricMode=rssi` queda operativo de punta a punta: la clase
`DvClRssiMetric` (25-jul) ahora recibe la medición real del enlace.

**El RSSI no toca el formato de aire.** Es una medición *local*: el receptor la
toma del tag del PHY al oír el beacon del vecino (`loraTag.GetReceivePower()`
en el camino pueyo7b, que es el que corre en campañas). Como el coste de camino
ya se propaga en el `score`, no hace falta añadir bytes al beacon — y el wire,
que está pinado con tests de bytes-dorados, queda intacto.

Confusor evitado: la histéresis de conmutación se aplicaba solo en modo
`COMPOSITE_SCORE`, así que RSSI habría corrido sin amortiguación y la
comparación habría mezclado *métrica* con *histéresis*. Se cambió a "toda
métrica que no sea `toa_only`", que deja idéntico el comportamiento de los dos
modos existentes.

Sonda (25 nodos, grid all-to-all, 20 ks, duty-on): las tres métricas producen
encaminamientos distintos, que es la condición para que el barrido mida algo.

| métrica | PDR | entregados | relevos | saltos medios |
|---|---:|---:|---:|---:|
| composite | 0.1004 | 4987 | 1656 | 0.064 |
| toa | 0.1019 | 5062 | 2175 | 0.102 |
| rssi | 0.1025 | 5090 | 1637 | 0.082 |

**Determinismo verificado como corresponde.** Primer intento mal planteado:
comparé contra las filas congeladas de E1, que salieron del **servidor**, desde
una corrida en **WSL** — comparación entre árboles, no determinismo. Rehecho en
un solo árbol (WSL, con y sin el cableado, vía `git stash`): idéntico dígito a
dígito en los cuatro casos de control (0.104/6242, 0.1057/6343, 0.0615/295,
0.0673/323). El cableado es aditivo y no altera composite ni toa. Suites 10/10.

**Regla de método que sale de ahí:** el árbol de WSL (gcc 13, ns-3.46.1-dev) y
el del servidor (gcc 15, ns-3.46) **no producen resultados idénticos** — mismas
entradas, PDR 0.104 vs 0.1218 en el mismo caso. Es esperable en un simulador
caótico donde diferencias de coma flotante cambian una decisión de ruta y
cascadean. Consecuencia: **todo dato del paper sale del servidor**; WSL es solo
para desarrollo y tests. Ya se cumplía (E1/E2 y E3 son del servidor), pero
conviene tenerlo escrito junto a la regla de no recompilar a mitad de lote.

## 2026-07-25 (c) — E3 completo: la métrica compuesta NO alarga la vida útil

240 celdas, 0 fallos, tráfico sostenido verificado en las 240 (`last_data_s`
≥ 250 ks en todas). {composite, toa} × {grid all-to-all, random convergecast} ×
N {25, 49, 100} × 20 semillas, 300 ks, duty-on 1%. Pareado por semilla.

| composite vs toa | media | t | a favor |
|---|---:|---:|---|
| **relevos** | **−28.02%** | **−51.40** | **120/120** |
| PDR | +1.71% | +12.10 | 105/120 |
| **FND (vida útil)** | **+0.03%** | +3.61 | 77/120 |
| T50 | −0.01% | −1.29 | 53/120 |
| energía TX | −0.02% | −4.68 | 49/120 |

**Q1 queda respondida en su propio terreno, y la respuesta es que no.** La
métrica compuesta reduce el relevo un 28% —en las 120 parejas, sin una sola
excepción— y aun así la vida útil no se mueve: +0.03% de FND son ~60 segundos
sobre 180 000. Es detectable estadísticamente y nulo en la práctica. T50 ni
siquiera es detectable. Estable por escenario (+0.03% en ambos) y por tamaño
(N=25/49/100).

El mecanismo ya estaba medido y aquí se confirma a escala: la compuesta lleva
coste explícito por salto (β=0.15) y `toa_only` no, así que prefiere caminos
cortos. Pero el total de transmisiones apenas cambia — **mueve transmisiones de
relevo a origen, no las elimina** — y por eso la energía TX no baja (−0.02%) y
el FND no responde. Consistente con Q3/Q5: bajo duty 1% el relevo es una
fracción pequeña del presupuesto, dominado por idle y balizado.

### Lo que sí gana: entrega durante la degradación

El PDR sube +1.71% (t=12.10, 105/120), y esto **contrasta con E1**, donde la
misma comparación dio −0.10% (nulo). Las dos campañas difieren en duración
(40 ks vs 300 ks) y en carga (100 vs 1400 paq/par), y solo en E3 los nodos
mueren. La lectura natural es que la ventaja aparece cuando la red se degrada:
la compuesta no retrasa la primera muerte, pero entrega mejor mientras la red
se va muriendo. **Es una hipótesis con confusor** —duración y carga cambian a
la vez— y separarlas exigiría una corrida larga sin agotamiento. No se afirma
como resultado.

Datos: `tools/validation/e3_results.csv`.

### Fallo de despliegue: las 480 celdas de RSSI de E1

Salieron todas con rc≠0. Causa: extendí el runner con `rssi` y lo encadené,
pero **no desplegué el binario nuevo al servidor**; su copia era del 24-jul y
aborta en la validación `routeMetricMode debe ser composite_score|toa_only`.
Error de método, no del simulador: código y binario del servidor deben
sincronizarse **antes** de encolar campañas que usen una función nueva. El
arreglo verifica con una sonda que el binario acepta `rssi` antes de relanzar
las 480.

## 2026-07-25 (d) — E1 completo con las cuatro métricas: RSSI gana en la rejilla

2880 celdas, 0 fallos (480 por métrica × 4 en CSMA/CAD, + 960 de E2 en ALOHA).
PDR pareado por semilla, duty-on 1%, 40 ks.

**Globalmente las cuatro métricas siguen siendo indistinguibles** (todas dentro
de ±0.5%, |t| máximo 2.65 sobre n=480). Pero por escenario aparece estructura, y
no la que yo esperaba:

| en grid all-to-all | media | t | a favor |
|---|---:|---:|---|
| **rssi vs toa** | **+1.96%** | **11.73** | 127/160 |
| **rssi vs composite** | **+1.22%** | **9.16** | 125/160 |

En convergecast y multisink todo es nulo. Es decir: **en el escenario de estrés
de malla, la línea de referencia de una sola capa (RSSI) entrega mejor que la
métrica compuesta cross-layer.** Consistente en los ocho tamaños de red: RSSI es
la más alta en cada N de `grid_a2a`, de 0.3112 en N=9 a 0.0083 en N=100.

### Una predicción mía que resultó falsa

En el DoE escribí que se esperaba «equivalencia ≈ ToA bajo SF-por-sensibilidad»,
razonando que si el SF se elige por sensibilidad entonces el ToA es función
escalonada del RSSI y ambas métricas ordenarían igual. **Es falso, y los datos
lo dicen sin ambigüedad**: rssi y toa coinciden exactamente en 4 de 480 celdas,
con |Δ| mediana 2.43% y p90 8.0%. Encaminan distinto.

La razón, en retrospectiva: el ToA acumulado a lo largo del camino no es una
función monótona del RSSI del último salto — el ToA suma sobre saltos y el RSSI
que mide esta métrica es por enlace. Dos caminos con el mismo RSSI de primer
salto pueden tener ToA de camino muy distinto, y viceversa. La equivalencia solo
valdría enlace a enlace, no camino a camino.

Consecuencia para el paper: RSSI **no** se puede descartar como redundante con
ToA. Es una línea de referencia legítima y, en la rejilla, mejor. Tampoco
procede ya el argumento de que «ToA precia el recurso regulado y RSSI no» como
si eso implicara ventaja de ToA en entrega: bajo duty 1% no la implica.

Datos: `tools/validation/e1e2_results.csv` (2880 filas).

## 2026-07-25 (e) — Flooding gestionado implementado (E6): baja el aire, no lo sube

Línea de referencia externa del DoE (decisión D1). Inundación estilo Meshtastic
sobre el mismo stack: `FloodingMode=true` desactiva la consulta de la tabla de
rutas; el origen difunde, cada vecino redifunde **una sola vez** —la dedup por
`{src,dst,seq}` de `m_seenOnce` ya existía y lo garantiza— hasta agotar el TTL.
Se marca `via=0xFFFF` para que todo receptor lo procese, en vez del filtro por
siguiente salto del plano DV. `FloodJitterMs` (500 ms por defecto) desincroniza
las redifusiones; sin él los vecinos redifunden a la vez y colisionan.

Sonda (25 nodos, grid all-to-all, 20 ks, duty-on 1%, seed 3):

| plano de datos | PDR | entregados | Tx origen | Tx relevo | **Tx datos total** | saltos |
|---|---:|---:|---:|---:|---:|---:|
| DV-CL (composite) | **0.1004** | **4987** | 22176 | 1656 | **23832** | 0.064 |
| flooding (jitter 500 ms) | 0.0677 | 3360 | 9177 | 14767 | 23944 | 0.725 |
| flooding (sin jitter) | 0.0662 | 3289 | 9035 | 15001 | 24036 | 0.721 |

**El total de transmisiones es prácticamente el mismo en los tres casos
(~24 000), porque el duty cycle lo acota.** Esa es la observación interesante:
bajo restricción regulatoria el flooding no puede «gastar más aire para entregar
más»; lo que hace es gastar el mismo presupuesto en redifusiones redundantes
(14 767 relevos frente a 1 656) a costa de las transmisiones de origen, que caen
de 22 176 a 9 177. Resultado: **entrega un 33% menos con el mismo coste**.

El jitter aporta poco aquí (0.0677 vs 0.0662): bajo duty 1% el propio gate ya
espacia las transmisiones, así que la contención extra apenas añade. Se mantiene
por realismo y porque en regímenes sin duty sí importa.

Determinismo: el modo DV no se altera (composite reproduce 0.1004 / 4987 / 1656
exactamente igual que antes del cambio). Suites 10/10.

Defecto propio corregido en el camino: la primera versión pasaba
`logTxMetrics=false` al difundir, así que las transmisiones del flooding no se
contaban y los contadores salían en cero — justo la magnitud que la comparación
mide. Ahora se registran y el colector las clasifica por `nodeId != src` como en
el plano DV.

## 2026-07-25 (f) — E6: DV-CL frente a flooding, con dos cruces opuestos

320 celdas, 0 fallos. Flooding gestionado × {grid all-to-all, convergecast} ×
N {9..100} × 20 semillas, duty-on 1%, comparado celda a celda contra las filas
de E1 (mismos escenarios, N y semillas).

**Global: DV-CL entrega +25.08% más que el flooding** (t=12.92, 235/320
semillas). Por escenario: +37.90% en la rejilla (t=18.62), +12.26% en
convergecast (t=4.10).

Pero el promedio esconde lo interesante. La razón DV/flooding por tamaño:

| N | grid all-to-all | convergecast |
|---|---:|---:|
| 9 | **1.55×** | 0.76× |
| 16 | 1.57× | 0.70× |
| 25 | 1.56× | 0.88× |
| 36 | 1.53× | 1.11× |
| 49 | 1.50× | 1.32× |
| 64 | 1.41× | 1.41× |
| 81 | 1.08× | 1.46× |
| 100 | **0.83×** | **1.49×** |

**Hay dos cruces, y van en sentidos contrarios.** En la rejilla DV domina hasta
N≈64 y el flooding lo alcanza y supera en N=100. En convergecast ocurre lo
inverso: el flooding gana claramente hasta N≈25 y DV se impone desde N≈36,
creciendo hasta 1.49×.

Lectura mecánica, coherente con el resto de los hallazgos:

- **Convergecast con pocos nodos**: casi todos alcanzan el único sumidero en uno
  o dos saltos (el flooding entrega con 1.33 saltos medios), así que la
  redundancia del flooding es barata y cubre los fallos de enlace, mientras DV
  apuesta por un camino único que a veces elige mal. Al crecer N la redundancia
  se vuelve prohibitiva bajo duty 1% y DV gana.
- **Rejilla con N=100**: la red está saturada (PDR 0.008–0.010 en ambos), y en
  régimen de colapso la redundancia del flooding rescata algo donde el camino
  único ya no llega. No es que el flooding escale mejor: es que ambos han caído
  y a esos niveles la comparación pierde sentido práctico.

Es un resultado más defendible que un «ganamos siempre»: los cruces tienen
mecanismo explicable y delimitan **cuándo** conviene cada plano de datos.

Cautela sobre la latencia: la media favorece a DV (−27.47%, t=−14.14) pero solo
la mitad de las celdas (160/320) lo hacen individualmente, así que la
distribución está sesgada por unas pocas celdas con diferencia grande. No se
afirma ventaja de latencia sin un análisis de la distribución.

Datos: `tools/validation/e6_results.csv`.

## 2026-07-25 (g) — DEFECTO: el término de ToA de la métrica compuesta satura

A raíz de una pregunta de Diego («¿seguro que las métricas se aplicaron bien,
que realmente se ruteaba por eso, que las balizas transmitían esa métrica?»)
se hicieron dos comprobaciones que faltaban. La primera confirma; la segunda
destapa un defecto que cambia la interpretación de E1.

### Lo que sí funciona: el ruteo ocurre por la métrica

Misma semilla, misma topología, mismo escenario, cambiando solo la métrica
(25 nodos, grid all-to-all, 15 ks). Comparando el **siguiente salto elegido**
para cada par (nodo, destino) sobre 600 entradas de ruta:

| comparación | pares distintos |
|---|---:|
| composite vs toa | 87.2% |
| composite vs hops | 84.0% |
| composite vs rssi | 83.8% |
| toa vs rssi | 83.5% |

Las métricas eligen rutas distintas en la gran mayoría de los pares, el score
viaja en la baliza y ninguna satura el techo del byte (0% en 255). El
mecanismo de enchufado funciona.

### El defecto: T̂ está clavado en 1

`NormalizeToa = min(toaUs / kMaxToaUs[sf], 1)`. Medido sobre las 6220 balizas
que alimentan la métrica en esa corrida:

| | p10 | p50 | p90 | saturados |
|---|---:|---:|---:|---:|
| **T̂ de balizas** | **1.000** | **1.000** | **1.000** | **90.3%** |
| T̂ de datos | 0.545 | 0.545 | 0.545 | 0.0% |

**El 90% de los enlaces tiene T̂ = 1**, así que el coste compuesto por salto es
constante: 0.6·1 + 0.15 = **0.75**, que cuantizado da q = 30. De ahí que los
scores anunciados por `composite` sean múltiplos exactos de 30 y tomen solo 7
valores distintos: son *saltos × 30*.

**Consecuencia: la métrica compuesta degenera en conteo de saltos.** Y eso
explica mecánicamente el resultado de E1 que quedó sin explicar: composite vs
hops dio +0.08% (t=0.51, nulo) porque **son efectivamente la misma métrica** en
este régimen.

Dos causas, ambas de modelado:

1. **El techo de normalización es demasiado bajo para el tamaño de baliza en
   uso.** Una baliza de ~251 B a SF7 ocupa ~408 ms de aire; `kMaxToaUs[SF7]` es
   143 360 µs. La razón es 2.85, luego se recorta a 1. Los `kMaxToaUs` venían
   heredados del árbol de campaña y el propio comentario del módulo decía que
   su origen absoluto «no afecta al ranking, solo a la escala de T̂». **Ese
   comentario es falso**: la escala decide si la normalización satura, y aquí
   satura.
2. **A la métrica se le da el ToA de la baliza, no el del dato.** El coste de
   un enlace debería reflejar lo que cuesta enviar *datos* por él, no lo que
   costó la baliza que lo anunció. Los datos van a SF8 con T̂ = 0.545, sin
   saturar: si la métrica se alimentara del ToA de datos, discriminaría.

### Qué queda en pie y qué no

**No afectado** (no dependen del término de ToA): el efecto del MAC
(+9.3% de PDR, CSMA/CAD vs ALOHA), el hallazgo del duty como ecualizador (Q5),
el aislamiento de δ·Ψ —que sí varía con el SoC y sí actúa—, la comparación con
flooding (E6), y todo E3 (donde composite vs toa mide otra cosa: la vía
`toaCostUnits` frente a la vía `rawMetric`).

**Afectado**: la interpretación de que «la métrica compuesta cross-layer no
mejora frente a métricas de una capa». Lo medido es que **una métrica compuesta
cuyo término de ToA está saturado** no mejora — que es un enunciado mucho más
débil y, sobre todo, un defecto corregible. Q1 debe re-evaluarse tras corregir
la normalización.

## 2026-07-25 (h) — Corrección de la normalización de ToA, y lo que destapa

Dos correcciones al término de ToA, más tres hallazgos que salieron al hacerlas.

### Corrección 1: la métrica recibía el ToA de la baliza, no el del dato

El coste de un enlace debe preciar lo que cuesta llevar **datos** por él. Se le
pasaba el airtime de la baliza que anunció la ruta, que es mucho mayor (una
baliza de ~257 B a SF7 son 408 ms; un dato de 27 B son 75 ms) y cuyo tamaño
depende de cuántas rutas lleve, no del enlace. `DvClRouting::SetDataToaForSf`
recibe de la aplicación el airtime de un dato por SF, calculado una vez al
arrancar con la carga útil y los parámetros de radio reales.

### Corrección 2: la normalización por SF no puede preciar el aire

Medido antes de elegir cómo corregir (carga fija de 27 B):

| normalización | T̂ de SF7 a SF12 | ¿discrimina? |
|---|---|---|
| techo heredado | 1.000 → 1.000 | no: **saturado** |
| por-SF con techo correcto | 0.204 → 0.226 | no: **varía 9%** |
| **techo global** | **0.0089 → 0.2256** | **sí: factor 25×** |

**Arreglar el techo no bastaba.** Con normalización por SF, numerador y
denominador escalan juntos, así que con carga fija T̂ es casi constante: el
término no puede preciar el aire por mucho que se ajuste el techo. Solo un
techo **global** hace que un enlace a SF12 cueste más que uno a SF7, que es la
señal cross-layer que la métrica dice llevar.

Implementado como atributo `ToaNormGlobal` (por defecto `true`) más
`ToaCeilingUs` (por defecto 8 462 336 µs = 222 B a SF12). El modo por-SF queda
disponible para reproducir la fórmula tal como está escrita en la tesis, y los
tests que la fijan ahora lo declaran explícitamente.

### Hallazgo A: con los pesos de la tesis, β domina al término de ToA

El test de convergencia «un desvío barato de dos saltos gana a un enlace
directo caro» **dejó de pasar**, y no porque el test estuviera mal:

- desvío rápido, 2 saltos: 2·(0.6·0.00118 + 0.15) = **0.301**
- directo lento a SF12: 0.6·0.2256 + 0.15 = **0.285**

Gana el directo **pese a gastar 95× más aire** (1.9 s contra 20 ms). El umbral
es β < 0.134; la tesis usa β = 0.15. Antes la propiedad se cumplía por
accidente: la saturación clavaba el enlace lento en el máximo. El test ahora
fija β = 0.05 y deja constancia de la tensión.

### Hallazgo B: el barrido de pesos del 22-jul queda invalidado

Aquel barrido concluyó que «α y β son inertes». Era un artefacto: con T̂ = 1
saturado, α multiplicaba una constante y β era otra constante, así que ninguno
podía hacer nada. Con la normalización corregida α y β sí tienen efecto — el
Hallazgo A lo demuestra directamente. **El barrido debe re-correrse**, y ahora
importa mucho más que antes.

### Hallazgo C: el paso de cuantización quedó grueso

Los costes son ahora ~10× menores, así que con `CompositeCostStep = 0.025` los
márgenes entre rutas alternativas caen a 2-3 unidades cuantizadas. No afecta a
las campañas —el perfil fija `routeSwitchMinDeltaX100 = 0`, sin histéresis— pero
sí al banco de test, que usa el default 5. Conviene revisar el paso junto con
los pesos.

### Estado

Suites 10/10. Las rutas siguen difiriendo entre métricas (83-88% de los pares
(nodo, destino)), y ahora por razones correctas. **Todos los resultados de
comparación de métricas (E1) quedan pendientes de re-correr**; los de MAC, duty
(Q5), energía y flooding no dependen del término de ToA.

## 2026-07-26 — El término de energía no se puede evaluar en corridas cortas

A raíz de otra pregunta de Diego («la métrica también considera la batería,
¿por qué no analizaste su peso?»). El piloto de pesos fijó δ=0.25 sin barrerlo,
y al ir a corregirlo apareció que el problema no era el barrido sino el
**régimen**: en una corrida perf de 40 ks el término δ·Ψ **no puede hacer
nada**.

Ψ(b) vale 0 por encima de b_hi = 0.50. Estado de carga al final de la corrida
(25 nodos, tráfico sostenido, duty 1%):

| duración | SoC mín | SoC p50 | SoC máx | nodos con Ψ>0 |
|---|---:|---:|---:|---:|
| **40 ks (perf)** | 0.472 | 0.594 | 0.862 | **4 de 25** |
| 150 ks | 0.107 | 0.254 | 0.518 | 23 de 25 |
| 300 ks | 0.000 | 0.000 | 0.049 | 25 de 25 |

**A 40 ks el término es inerte por construcción**: casi nadie cruza el umbral,
así que δ multiplica cero y barrerlo no mide nada. Cualquier conclusión sobre δ
sacada de corridas perf es vacía.

Y hay un segundo filo: **a 300 ks todos terminan en 0**, es decir Ψ saturado al
máximo para todos — otra constante. El régimen donde δ discrimina es el
intermedio (~150 ks), donde el SoC está repartido a lo largo de la rampa
[b_lo, b_hi] = [0.20, 0.50]: mín 0.11, p50 0.25, máx 0.52.

Esto matiza hacia atrás el resultado de E3: la comparación a 300 ks mide δ en
un régimen donde acaba saturado, así que subestima su efecto. No lo invalida
—el FND se decide antes de que todos mueran— pero conviene tenerlo presente.

**Consecuencia de diseño.** Los pesos no se pueden optimizar en dos barridos
separados: α y β se ven en PDR, pero δ solo aparece cuando la batería entra en
la rampa. Se añade `run_e5_joint.sh`, que corre 300 ks y mide **PDR y FND en la
misma corrida**, barriendo α, β y δ juntos con `toa_only` como referencia en
las mismas semillas. Es lo único que responde el criterio de Diego: superar a
toa_only en entrega **y** en batería.

## 2026-07-27 — Verificación previa al barrido, y un límite del escenario

Antes de gastar horas barriendo δ se comprobó que puede actuar. Corridas de
150 ks (el régimen discriminante), 25 nodos, α=0.6, β=0.05:

| comprobación | resultado |
|---|---|
| SoC anunciado en balizas | p10=0.347, p50=0.567, p90=0.791; **35% dentro de la rampa** [0.20, 0.50] |
| ¿δ cambia rutas? | **80.2%** de los 600 pares (nodo,destino) difieren entre δ=0 y δ=0.25 |
| resolución del score | 20-55 valores distintos, solo 4-5% en score ≤ 2 |

El término de energía tiene material sobre el que actuar y actúa. El barrido
conjunto tiene sentido.

### Límite del escenario: casi no hay diversidad de SF

Medido sobre las transmisiones de esa misma corrida: **SF7 el 17%, SF8 el 83%,
y nada más**. La rejilla de 178 m con 20 dBm da enlaces buenos, y el SF por
sensibilidad se queda en la parte baja del rango.

Consecuencia directa sobre el término de ToA: solo puede distinguir entre dos
valores de T̂ (0.039 y 0.073 con el techo derivado), un factor de 1.9. Por muy
bien calibrado que esté, **en este escenario el término de ToA tiene poco que
discriminar**, mientras que β es constante por salto. Eso acota cuánto puede
separarse la métrica compuesta del conteo de saltos, con cualquier peso.

No es un defecto del simulador ni de la métrica: es una propiedad del
escenario. Pero conviene decirlo en el paper y, si se quiere ejercitar el
término de ToA, hace falta un escenario con enlaces largos que fuercen SF10-12
(por ejemplo `pueyoGridSpacingM` mayor, o menos potencia de transmisión).

## 2026-07-27 — DEFECTO RAÍZ: el SF de datos no depende del enlace

Diego señaló que esperaba diversidad de SF en topología aleatoria por las
distancias. Al medirlo apareció algo peor que la falta de diversidad.

### Lo medido

SF de los paquetes de **datos**, por escenario (20 ks, duty 1%):

| escenario | SF de datos |
|---|---|
| rejilla 178 m, N=25 y N=100 | **100% SF8** |
| aleatoria 1 km, N=25 y N=100 | **100% SF8** |
| aleatoria 3 km, N=25 | **100% SF8** |
| aleatoria 5 km, N=49 | **100% SF8** |

Ni la topología ni el área cambian nada. Y al abrir el rango a SF7-12 con
`--allowPaperLikeSfRangeVariant`, pasa a ser **100% SF12** en los cinco
escenarios: siempre el máximo permitido.

### La causa

`GetDataSfForNeighbor` devuelve **SF12 como valor de reserva** cuando no
encuentra un SF reciente para el vecino, y después se recorta a `sfMax`. Eso da
exactamente lo observado: sfMax=8 → SF8; sfMax=12 → SF12.

Comprobado con el registro de depuración (9 nodos, 3 ks):

| | |
|---|---|
| Llamadas que caen al valor de reserva | **2716 de 2716 (100%)** |
| Motivo, para todos los vecinos | «no recent SF» |
| `UpdateNeighborLinkSf` | «no robust SF yet (mode=robust_min minSamples=2)» |

La condición para tener un SF vigente es haber oído al vecino dentro de una
ventana igual al intervalo de baliza (60 s) y, en modo `robust_min`, con al
menos 2 muestras del mismo SF dentro de ella. Cada vecino baliza una vez cada
60 s, así que la condición es estructuralmente casi insatisfacible — y bajo
duty 1%, con las balizas retrasadas, nunca se satisface.

### Por qué importa

**Si todos los enlaces de datos usan el mismo SF, el ToA de referencia es
idéntico para todos y el término α·T̂ es una constante.** La métrica compuesta
se reduce a `constante + β + δ·Ψ`, es decir conteo de saltos más energía, y no
puede preciar el aire por muy bien normalizado que esté.

Esto es más profundo que el defecto de saturación corregido el 25-jul. Aquella
corrección era necesaria y sigue siendo correcta, pero **no puede surtir efecto
mientras el SF de datos sea constante**. También explica que el piloto de pesos
mostrara diferencias entre α que solo venían de la cuantización.

### Consecuencias

- El término de ToA **nunca ha estado operativo** en ninguna campaña.
- Toda comparación `composite` vs `hops` mide dos métricas que son la misma
  salvo el término de energía.
- El barrido de pesos en curso mide α sobre una constante: **α no puede
  optimizarse hasta arreglar esto**.
- No afecta a: efecto del MAC, Q5 (duty), presupuesto de energía, mecanismo de
  la cola del SoC, ni al término δ·Ψ, que sí varía con el SoC del vecino y sí
  actúa (verificado 2026-07-27: cambia el 80.2% de las rutas).

### Arreglo (2026-07-27): el SF de datos lo decide la sensibilidad del enlace

Diagnóstico fino, con instrumentación de las ramas de `GetDataSfForNeighbor`:
el 100% de las llamadas terminaba en la reserva devolviendo **12**, porque
`lastSeenBySf` estaba vacío para todo el rango y `lastRxSf` valía 12. Es decir,
`UpdateNeighborLinkSf` se llamaba siempre con `rxSf = m_sfControl` (12): **el SF
que el receptor lee del paquete no llega**, aunque el RSSI del mismo tag sí.

La vía fiable es el RSSI. `ComputeMinSfBySensitivity` ya estaba bien
implementado (tabla del SX1276 a 125 kHz, devuelve el SF mínimo que supera la
sensibilidad) y `ResolveSfForLink` ya lo usaba para el registro de enlace del
ruteo. Solo faltaba que el plano de datos lo consumiera.

Se añade `AppNeighborLink::sensitivitySf`, que se rellena al recibir baliza con
el SF exigido por la sensibilidad de ese enlace, y `GetDataSfForNeighbor` lo usa
como primera opción.

**Verificación tras el arreglo** — el SF ahora depende del enlace y de la
distancia:

| escenario | SF de datos |
|---|---|
| rejilla 178 m, N=25 | SF7 84%, SF8 16% |
| aleatoria 1 km, N=25 | SF7 85%, SF8 15% |
| aleatoria 3 km, N=25 | SF7 46%, SF8 54% |
| **aleatoria 5 km, N=25** | **SF7 39%, SF8 61%** |

Y con el rango completo 7-12 aparece el abanico entero:

| escenario (SF 7-12) | SF7 | SF8 | SF9 | SF10 | SF11 | SF12 |
|---|---|---|---|---|---|---|
| rejilla 178 m | 56% | 23% | 13% | 7% | 0% | 0% |
| aleatoria 3 km | 48% | 17% | 18% | 14% | 1% | 2% |
| **rejilla dispersa 600 m** | 12% | 27% | **32%** | **25%** | 2% | 1% |

Con esto el ToA de referencia deja de ser constante y el término α·T̂ puede
por fin preciar el aire, que era el propósito del diseño cross-layer. Suites
10/10.

### Parche al PHY: el receptor etiqueta el SF que demoduló, y comprobación cruzada

Diego preguntó por qué el SF no llega al receptor si el propio receptor sabe
qué está demodulando. Tenía razón: lo sabe y no lo escribía.

Comparando los dos PHY del módulo `lorawan`:

- `SimpleEndDeviceLoraPhy::Send` **sí** etiqueta el paquete con su SF.
- `SimpleGatewayLoraPhy::Send` **no** lo hace — y nuestros nodos usan el de
  gateway. Además, en la recepción correcta el PHY escribe potencia y
  frecuencia en el `LoraTag` (`SetReceivePower`, `SetFrequency`) pero **no el
  SF**, pese a tenerlo en `event->GetSpreadingFactor()`.

Eso explica que el RSSI llegara bien y el SF no. Parche de una línea, junto a
los otros dos que ya aplicamos a `lorawan`, en
`tools/validation/server_setup_lorawan.sh`:

    tag.SetSpreadingFactor(event->GetSpreadingFactor());

### La comprobación cruzada: consistentes, y no idénticos (como debe ser)

Con el parche puesto se comparó, en cada baliza recibida, el SF **demodulado**
(tag) contra el SF **mínimo que la sensibilidad exige** para ese enlace (sens),
derivado del RSSI:

| rango | n | coinciden | discrepancias observadas |
|---|---:|---:|---|
| [7,8] | 8760 | 68.2% | siempre `tag=SF8, sens=SF7` |
| [7,12] | 2204 | 41.3% | siempre `tag > sens` (p.ej. tag=SF9, sens=SF7) |

**No se observó ni un solo caso de `tag < sens`**, que sería una contradicción
física: no se puede demodular en un SF que el enlace no soporta. Las dos vías
son consistentes.

Y no debían coincidir: miden cosas distintas. `tag` es el SF que el emisor
eligió *para esa baliza* (rotan con la PMF geométrica, sin saber quién
escucha); `sens` es el SF mínimo que *este* enlace admite. Un enlace bueno oye
al vecino en muchos SF, y el mínimo de ellos es el que conviene usar para
datos, porque es el que menos aire gasta. Por eso el plano de datos usa `sens`
y no `tag`: el RSSI es propiedad del enlace (no depende del SF), así que la
tabla de sensibilidad da la respuesta correcta sea cual sea el SF en que llegó
la baliza.

### El modelo de recepción, y qué asume Pueyo-Centelles

Consulta a NotebookLM (`Papers_Magister`, 2026-07-27) sobre cómo resuelve
Pueyo-Centelles 2024 el mismo problema. **Asumen transceptores LoRa de un solo
canal** (tipo ESP32) con **detección automática del SF por preámbulo/CAD**: el
chip detecta el SF de la transmisión entrante y **se reconfigura al vuelo** para
decodificarla, con la limitación física de **un paquete a la vez**.

Es exactamente lo que modela nuestro `SimpleGatewayLoraPhy` con **una sola ruta
de recepción**: la clase `ReceptionPath` no tiene campo de SF —se engancha a un
evento, no a un factor— así que puede recibir cualquier SF, pero solo uno a la
vez. **El modelo coincide con el supuesto del paper ancla**, lo que conviene
declarar como nota al pie: un end-device puro con el demodulador fijado a un SF
sería sordo a los demás, y la malla multi-SF necesitaría otro mecanismo.

(Nota: el árbol tiene `enableSfScanRx` y `sfScanEdThresholdDbm`, o sea que el
escaneo explícito llegó a implementarse, pero el perfil lo deja en `false`; el
PHY ya da el comportamiento equivalente.)

## 2026-07-27 (b) — Barrido conjunto con la métrica ya operativa: hay disyuntiva

Primer barrido de pesos con el término de ToA funcionando (SF por enlace, techo
derivado, ToA de datos). 560 celdas, 0 fallos: α{0.2,0.6,1.0} × β{0.02,0.05,
0.15} × δ{0,0.25,0.5} × N{25,49} × 10 semillas, más `toa_only` de referencia en
las mismas celdas. Corridas de 300 ks, duty 1%, pareado por semilla y N.

### El resultado, sin adornos

**Las 27 combinaciones superan a `toa_only` en PDR. Las 27 pierden en vida
útil.** No hay ni una que gane en ambos.

| | rango | significación |
|---|---|---|
| ΔPDR | **+0.72% a +2.38%** | t de 3.3 a 10.9, todas a favor |
| ΔFND | **−1.69% a −2.23%** | t de −31 a −49, todas en contra |
| ΔT50 | −1.54% a −2.12% | mismo patrón |
| relevos | −29% a −44% | la compuesta releva mucho menos |

Mejor PDR: **α=1.0, β=0.05, δ=0** → +2.38% de PDR (t=10.9), −1.69% de FND.

### δ empeora las dos cosas

Promediando sobre α y β:

| δ | ΔPDR medio | ΔFND medio |
|---|---:|---:|
| 0.00 | **+1.68%** | **−1.84%** |
| 0.25 | +1.16% | −2.05% |
| 0.50 | +1.08% | −2.08% |

Subir el peso de la energía **reduce el PDR y acorta la vida útil**, de forma
monótona. Esto **contradice** el hallazgo del 2026-07-22 (δ daba +9.69% de FND
sin duty), pero aquel se midió con el término de ToA saturado, o sea con una
métrica que era conteo de saltos: no es comparable.

### El mecanismo, en parte

Comparando niveles absolutos (N=25, semilla 1) contra `toa_only`:

| | PDR | FND | TX mAh | relevos | Tx origen |
|---|---:|---:|---:|---:|---:|
| toa_only | 0.1006 | 200957 | 2003.6 | 60367 | 568496 |
| α=1.0 β=0.05 δ=0 | 0.1017 | 196518 | 1993.0 | 37866 | 534975 |

**La compuesta gasta MENOS energía de transmisión en total (−0.4 a −0.55%) y
aun así el primer nodo muere antes.** Menos energía total pero peor FND solo
puede significar que la carga se **reparte peor**: el coste de camino se
minimiza concentrando el tráfico en los nodos bien situados, y el FND lo fija
el que más aguanta el peso.

**Es una hipótesis, no un hecho medido**: el CSV agregado suma la energía de
todos los nodos y no permite ver la dispersión. Para confirmarlo hace falta el
desglose por nodo, que el runner descarta. `soc_min` no sirve aquí porque a
300 ks todos terminan en 0 (saturado).

### Qué decisión fuerza

El criterio pedido —superar a `toa_only` en PDR **y** en batería— **no lo
cumple ninguna combinación de esta rejilla**. Las opciones son: aceptar la
disyuntiva y elegir punto de operación, buscar fuera de la rejilla, o entender
primero por qué se concentra la carga, que puede revelar otro defecto o un
resultado de fondo.

Datos: `tools/validation/e5_joint.csv`.

## 2026-07-27 (c) — El mecanismo de la paradoja, y un confusor que la explicaba en parte

Diagnostico de dispersion: 48 celdas a 150 ks (N=25, 8 semillas, toa_only vs
compuesta con delta 0 y 0.5, dos brazos de bateria). 150 ks y no 300 ks porque
a 300 ks todos acaban en SoC 0 y no se mide dispersion sobre una constante; a
150 ks nadie ha muerto y el consumo por nodo es medida limpia. Datos:
`tools/validation/e5_dispersion.csv`.

### Las tres hipotesis

| | veredicto |
|---|---|
| H1 concentracion de carga | **falsa** |
| H2 coste por transmision | **confirmada, dominante** |
| H3 artefacto de supervivencia | **confirmada** |

**H1 descartada.** El CV del consumo por nodo sube un 55% y el Gini un 50%,
que suena mucho, pero en absoluto va de 0.00036 a 0.00055: ambos son cero a
efectos practicos. En rejilla all-to-all la carga se reparte casi
perfectamente. El nodo mas cargado sube +0.78% y la media +0.75%: sube todo el
mundo por igual, no se redistribuye nada.

**H2 es el mecanismo.** La compuesta hace **8.8% menos transmisiones** de datos
y cada una cuesta **11.0% mas de airtime**, con lo que el airtime total sube
**1.26%**. El reparto de SF lo dice todo: SF8 pasa del **1.7% al 14.6%** de las
transmisiones, y los saltos por transmision caen un **34%**. Cambia cadenas de
saltos cortos en SF7 por saltos largos en SF8. Menos saltos explica el PDR
mejor; mas airtime explica la muerte antes.

Control: el SF medio y el airtime de las **balizas** coinciden entre brazos
(7.3326/7.3310 y 11502/11466 s). Su SF lo rota una PMF geometrica ajena al
ruteo, asi que si difiriesen habria un problema de instrumentacion.

**H3 confirmada, y anula un dato anterior.** A 150 ks, con nadie muerto, la
compuesta gasta **+0.75% de TX y +1.70% de energia total**. El -0.4% que se
midio a 300 ks era sesgo de supervivencia: sus nodos morian antes y dejaban de
gastar, asi que acumulaban menos.

### El confusor: la histeresis dependia del modo de metrica

Buscando por que dFND no respondia a los pesos —entre -1.69% y -2.23% sobre un
rango de 12.5x en beta y 5x en alfa— aparecio la causa. En
`dv-cl-routing.cc` la amortiguacion de conmutacion era:

```cpp
const bool applyHysteresis = (m_metricMode != MetricMode::TOA_ONLY);
```

`toa_only` era el **unico** modo que cambiaba de ruta ante cualquier mejora
bruta; los demas exigian que la mejora sobreviviese a la cuantizacion del
score. Como toa_only es la referencia de toda comparacion de ruteo, cada
comparacion mezclaba la formula de la metrica con la pegajosidad de la ruta. Y
como el gate ignora los pesos, el sesgo era plano sobre toda la rejilla, que es
justo lo que lo hacia parecer una propiedad de la metrica compuesta.

Viene del commit c329f6da8 (cableado de RSSI): ahi se detecto el confusor entre
RSSI y compuesta y se extendio la histeresis a "toda metrica que no sea
toa_only", lo que arreglo esa comparacion y dejo fuera justamente a la
referencia.

Corregido: atributo `RouteSwitchHysteresis` (por defecto true) en `DvClRouting`
y `DvClApp`, flag `--routeSwitchHysteresis`, aplicado por igual a todos los
modos. Prueba `DvClRoutingHysteresisIsModeAgnosticTestCase`, verificada por
control negativo: falla al reintroducir el gate. Suites 134/134.

### Que queda en pie y que no

El mecanismo H2/H3 es **independiente del confusor**: describe lo que hacen las
rutas, no de que arm vienen. Lo que **no** se puede atribuir todavia es la
causa: si el intercambio saltos-por-SF lo produce la formula de la metrica o la
pegajosidad de la ruta. Lanzado el 2x2 metrica x histeresis (32 celdas, 300 ks)
para separarlo; hasta que cierre, **las cifras del barrido conjunto de hoy no
son atribuibles a la metrica**.

## 2026-07-27 (d) — 2x2 metrica x histeresis: cuanto era el confusor

32 celdas, 300 ks, N=25, 8 semillas, alfa=1.0 beta=0.05 delta=0. Datos:
`tools/validation/e5_hyst2x2.csv`.

### Descomposicion

| | reportado (confundido) | **de la metrica** | de la histeresis |
|---|---:|---:|---:|
| dPDR | +1.508% (t=12.2) | **+0.980%** (t=8.0) | +0.524% (t=5.3) |
| dFND | -1.733% (t=-19.5) | **-1.733%** (t=-20.6) | +0.001% (t=0.0) |

**La perdida de vida util es enteramente de la metrica.** La histeresis aporta
un 0% y el efecto sale igual con las dos ramas amortiguadas (-1.733%) que con
las dos libres (-1.755%). Lo que si estaba inflado es el PDR: el confusor
aportaba un 35%, asi que la ganancia real es **+0.98%, no el +2.38%** del
barrido conjunto.

El mecanismo tambien es de la metrica: el reparto de SF es identico con y sin
amortiguacion (SF7/SF8 = 85.4/14.6 en ambas), mientras que entre metricas va de
98.3/1.7 a 85.4/14.6. La histeresis mueve los saltos por transmision un 1-2.6%;
la metrica los mueve un -33%.

### Lo que queda establecido

La metrica compuesta **compra PDR con vida util**, y el precio esta medido:
+0.98% de PDR a cambio de -1.73% de FND, via un intercambio de saltos por
spreading factor (-33% saltos, SF8 del 1.7% al 14.6%, +1.19% de airtime total).

### Donde esta la palanca (en curso)

El barrido original no vio efecto marginal de beta entre 0.02 y 0.15, y la
aritmetica dice por que: el termino de ToA normalizado vale ~0.03 en SF7 y
~0.055 en SF8, y el cuantizador COST255 usa paso 0.025. Con beta=0.02 un salto
SF7 cuesta q=2 y uno SF8 q=3, asi que dos saltos SF7 (4) pierden contra un SF8
(3); con beta=0.15 pierden 14 contra 8. **La rejilla entera estaba por encima
del punto de cambio**, por eso beta parecia inerte.

Lanzado `run_e5_beta_low.sh` (56 celdas): beta en {0, 0.005, 0.01, 0.02} con el
paso por defecto, mas paso en {0.010, 0.005} a beta fijo. beta=0 con alfa=1 es
ToA reescalado, o sea la costura contra toa_only.

## 2026-07-27 (e) — `toa_only` no es la compuesta con pesos a cero: son dos subsistemas

56 celdas, 300 ks, N=25, 8 semillas. Datos: `tools/validation/e5_beta_low.csv`.

### La costura falla

Con alfa=1, beta=0 y delta=0 la formula compuesta se reduce a `T_hat =
toa/techo`, o sea ToA reescalado. Un reescalado monotono positivo **no puede
cambiar el orden de las rutas**, asi que esa configuracion tendria que
reproducir `toa_only`. No lo hace:

| beta | paso | dPDR | dFND | SF8 | saltos | airtime |
|---:|---:|---:|---:|---:|---:|---:|
| 0.000 | 0.0250 | +1.15% | -1.69% | 14.0% | -32.5% | +1.12% |
| 0.005 | 0.0250 | +1.10% | -1.68% | 14.0% | -32.6% | +1.10% |
| 0.010 | 0.0250 | +1.28% | -1.71% | 14.0% | -32.6% | +1.10% |
| 0.020 | 0.0250 | +0.41% | -1.88% | 15.8% | -32.6% | +1.26% |
| 0.050 | 0.0100 | +1.07% | -1.75% | 14.6% | -33.2% | +1.19% |
| 0.050 | 0.0050 | +0.40% | -1.97% | 16.5% | -35.3% | +1.33% |
| *toa_only* | | *ref* | *ref* | *1.7%* | *ref* | *ref* |

beta=0 se aparta de la referencia tanto como beta=0.05. **beta no es la
palanca, y el paso del cuantizador tampoco** (afinarlo lo empeora). Sigue sin
haber ninguna combinacion que gane en PDR y en FND a la vez.

### La causa: dos tuberias de coste distintas

`toa_only` no pasa por la metrica enchufable. Es una rama entera aparte
(`dv-cl-routing.cc` lineas 1162, 1293, 1541, 1649) con:

```cpp
ToaHopCostUnits(sf) = 1u << (sf - 7);   // SF7..SF12 -> 1,2,4,8,16,32
```

coste entero por salto, acumulacion exacta en enteros, codificacion de anuncio
propia (`ToaUnitsToAdvertMetric` / `AdvertMetricToToaPathUnits`) y desempate
propio que **prefiere el SF mas bajo**. La compuesta usa el ToA real medido,
cuantizado con paso 0.025.

El modelo entero de potencias de dos se aparta del airtime real hasta un 26%:

| SF | ToA real | ratio vs SF7 | modelo `2^(sf-7)` | error |
|---:|---:|---:|---:|---:|
| 7 | 58.62 ms | 1.000 | 1 | 0.0% |
| 8 | 107.01 ms | 1.825 | 2 | **-8.7%** |
| 9 | 193.54 ms | 3.301 | 4 | -17.5% |
| 10 | 346.11 ms | 5.904 | 8 | -26.2% |
| 11 | 692.22 ms | 11.808 | 16 | -26.2% |
| 12 | 1384.45 ms | 23.616 | 32 | -26.2% |

De ahi sale la divergencia concreta. Un salto SF8 directo cuesta 107.01 ms y
dos saltos SF7 cuestan 117.25 ms: **en airtime real gana el SF8 por un 8.7%**.
El modelo entero los declara empate (2 = 2) y su desempate por SF bajo escoge
la cadena SF7. Por eso `toa_only` sale con el 98.3% de las transmisiones en SF7
y la compuesta con el 85.4%.

### Que significa para el DoE

La pregunta Q1 compara "nuestra metrica compuesta contra metricas de una sola
capa (ToA, saltos, RSSI, SNR)". Tal como esta, **la comparacion contra ToA no
enfrenta dos formulas: enfrenta dos subsistemas de coste**, que difieren en la
formula, en las unidades, en la cuantizacion, en la codificacion del anuncio y
en el desempate. El -1.73% de FND no es atribuible a la formula compuesta
mientras eso siga asi.

Decision pendiente con Diego, ver DOE.md.
