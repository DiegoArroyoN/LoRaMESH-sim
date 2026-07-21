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
