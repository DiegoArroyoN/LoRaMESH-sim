# DOE.md — Diseño de experimentos (F3.1), borrador para coautores

**Estado: BORRADOR v3, 2026-08-03.** Para revisión con S. Sobarzo y G. Saavedra
(gate G4). Reescribe la v2 del 2026-07-24: entre esa fecha y hoy se corrieron
nueve campañas nuevas (E13–E20, ~4 400 celdas) que **invalidan cuatro de las once
figuras del storyboard anterior** y desplazan el resultado principal del paper.
Las métricas se definen en `METRICS.md` (F3.2); la evidencia está en
`VALIDATION.md` con sus datasets en `tools/validation/`.

> **Qué leer si solo se tiene diez minutos:** §0 (qué cambió y por qué),
> §1 (preguntas reformuladas) y §11 (storyboard v1 propuesto). El resto es
> soporte.

---

## 0. Qué cambió desde la v2, y por qué

La v2 estaba construida alrededor de una hipótesis que resultó falsa. Conviene
decirlo sin rodeos porque condiciona todo lo demás.

**El término de energía δ·Ψ(b_j) no mejora ninguna métrica energética, en
ninguna condición alcanzable.** No es que el efecto sea pequeño: es que no
existe, y sabemos por qué.

| lo que la v2 daba por hecho | lo que se midió |
|---|---|
| δ redistribuye la cola del SoC y satura en δ≈0.10 | δ **nunca** mejora FND, T50, `soc_min` ni `e_max`, con δ de 0 a 0.85 |
| el duty cycle es un "ecualizador" que acota el margen de δ | el margen no existe con duty ni sin él; la explicación es otra |
| hay sinergia MAC×ruteo | la interacción es **negativa** (−8.21 pp) |
| el ruteo gobierna ~1.4% del gasto | **1.61%**, y como máximo 8.21% en el mejor de 72 escenarios |

**El mecanismo, medido:** δ sí hace su trabajo — reparte la carga de relevo
hasta un 22% mejor, en 40 de 40 celdas, p<0.0001. Lo consigue **estirando** los
enlaces: sube el SF, con lo que más nodos quedan al alcance y la carga se
reparte entre más candidatos. Pero cada transmisión en un SF más alto dura el
doble, así que el aire total sube, el consumo medio sube 0.54% y **el nodo más
cargado acaba peor que sin δ**. El equilibrio que compra cuesta más de lo que
ahorra.

De ahí la frase que gobierna el paper:

> **El PHY premia el SF alto por robustez y la regulación lo castiga por
> ocupación, así que cualquier término de la métrica que empuje hacia arriba en
> SF paga en el otro lado.**

**Y un hallazgo nuevo que desplaza al anterior como resultado principal.**
Espaciar las balizas de 60 s a 900 s da **+229% de PDR y +8.1% de FND**; la
métrica de ruteo, en el mismo escenario, da +4.7% de PDR a costa de −13.2% de
FND. A 60 s las balizas consumen **el 74.9% del aire**. La decisión de
planificación vale unas cincuenta veces más que la de encaminamiento, y mejora
las dos métricas a la vez mientras el ruteo tiene que elegir entre ellas.

Consecuencia incómoda que hay que asumir: **la ventaja de la métrica compuesta
se midió en un régimen mal dimensionado.** Pasa de +13.08% de PDR (balizas 60 s)
a +4.8% en cuanto la cadencia es sana, mientras su coste en vida útil se
triplica. El canje se degrada 7.6 veces.

**Fiabilidad del instrumento.** Antes de nada de lo anterior se auditaron los
147 parámetros del binario uno a uno (§10). Se encontraron y arreglaron ocho
que se aceptaban por línea de comandos y se descartaban en silencio — entre
ellos `--enableDuty` y `--enableCsma`, o sea dos de los ejes de este DoE.
Ninguna campaña los había usado; el fallo estaba en la vía que aún no habíamos
tomado.

---

## 1. Preguntas de investigación e hipótesis

**Los claims se derivan de los datos, no al revés** (decisión de Diego,
2026-07-24; se mantiene). Q2 y Q5 de la v2 giraban alrededor de δ y están
respondidas en negativo; se reformulan hacia la pregunta que los datos sí
contestan.

| # | Pregunta | Bloque | Estado de la evidencia |
|---|---|---|---|
| **Q1** | ¿La métrica compuesta cross-layer mejora PDR/vida útil frente a métricas de una capa, bajo duty EU868 1%? | M1 | parcial: +4.8% PDR / −13.2% FND en régimen sano (E20); falta el barrido completo por escenario y N |
| **Q2** | ¿Qué capa gobierna la vida útil de la red, y por qué **no** es el encaminamiento? | contexto, M2 | **respondida**: reposo 45.7%, balizas 21.7%, relevo 1.6%. E14 (320 celdas), sonda de 72 escenarios |
| **Q3** | ¿Por qué falla el término energy-aware, y bajo qué condiciones fallaría cualquier otro? | E17–E19 | **respondida**: mecanismo de escalada de SF, medido en 3 topologías y 2 rangos (E19, 720 celdas) |
| **Q4** | ¿Depende del MAC el beneficio de la métrica? ¿Hay interacción? | M1 | interacción **negativa** (−8.21 pp). Falta confirmarla en el diseño factorial |
| **Q5** | ¿Cuánto del rendimiento lo fija el **plan de control** frente a la métrica de ruteo? | M2 | **respondida en un escenario**: cadencia +229% PDR vs métrica +4.7%. Falta generalizar a 3 escenarios |
| **Q6** | ¿Es el umbral de SF de Pueyo-Centelles un efecto real o un artefacto de canal determinista? | E7 (m1) | **RESPUESTA CAMBIADA 2026-08-09**: no hay umbral. El colapso a PDR 0.0001 era artefacto de NUESTRO margen 0, no de su ecuación (4). A margen 1 el selector sube a SF8 y el PDR es 0.0151; la caída de 177→247 m es suave (×0.53) e **igual en los dos canales**, así que el sombreado no enmascara nada |

Q5 es ahora la más citable. Cotrim & Margi (2024) estudian el duty cycle como
cuello de botella de throughput; nadie ha caracterizado que **el plan de balizas
domine sobre la métrica de ruteo en un orden de magnitud**, ni ha señalado que
evaluar una métrica con una cadencia mal dimensionada mide sobre todo la
cadencia. Es un resultado metodológico además de un resultado de ingeniería.

Q6 es un segundo hallazgo defendible: la ecuación (4) de Pueyo-Centelles
(`h_lm = 2^(SF_lm − SF_min)`) hace que subir un SF **duplique** el coste, así
que `toa_only` prefiere fallar antes que escalar. A 177 m —su punto de
operación— eso da igual porque SF7 alcanza; a 247 m se hunde. El sombreado lo
enmascara: convierte un fallo catastrófico (250×) en una degradación suave (2×).

---

## 2. Régimen principal declarado

**Duty-on 1% por nodo, disciplina `time_off_air` (ETSI).** Sin cambios respecto
a la v2, y ahora con más motivo: el duty cycle es lo que pone el techo
estructural al gasto de transmisión (§0).

**Cambio respecto a la v2 — la cadencia de balizas pasa a ser factor, no
constante.** La v2 la fijaba implícitamente en 60 s (valor del perfil). E20
demuestra que ese valor domina el resultado, así que congelarlo sería medir un
artefacto. Se declara **900 s como punto de operación** (captura el 94% de la
ganancia de PDR y el 85% de la de FND frente a 60 s) y se reporta la curva
completa.

Consecuencia asumida, igual que en la v2: **todo resultado con balizas a 60 s
caracteriza un régimen saturado de control**, no el régimen principal.

---

## 3. Factores y niveles

| Factor | Niveles | Palanca verificada |
|---|---|---|
| Métrica | composite / toa_only / hops (α=0,β=1,δ=0) / rssi | `--routeMetricMode` (+ `--allowMetricModeOverride` para toa_only) |
| MAC | CSMA/CAD / ALOHA | **el perfil** (`..._csmacad` / `..._aloha`) |
| Duty | 1% (principal) / off / DC% ∈ {2,5,10} solo en E4 | presencia de `--allowDutyOverride=true`; nivel con `--dutyOverridePct` |
| **Cadencia de balizas** | **60 / 300 / 900 / 1800 s** (900 = principal) | `--allowBeaconOverride` + `--beaconIntervalWarmSec/StableSec` |
| Rango de SF | 7-12 (principal) / 7-8 (malla forzada) | `--allowPaperLikeSfRangeVariant` + `--sfMin`/`--sfMax` |
| Escenario | grid×all-to-all / random×convergecast-1-sink / random×multisink-4 | `--nodePlacementMode`, `--trafficMode`, `--forcedDataDestinationId`, `--numSinks` |
| N | 9, 16, 25, 36, 49, 64, 81, 100 | `--nEd` (cuadrados perfectos: `pueyo_grid` los exige) |
| Semillas | 20 (rngRun 1–20), **pareadas por semilla** | `--rngRun` |
| Batería inicial | U[60,100]% (estándar); igual-para-todos en el bloque de mecanismo | `--socInitMin` / `--socInitMax` |

**δ deja de ser factor.** Se fija en 0 y el resultado que lo justifica pasa a
ser una contribución del paper, no un parámetro por elegir. Las cuotas 0.25 y
0.85 sobreviven solo en el bloque de mecanismo (M3), como demostración.

Sustento de literatura (NotebookLM `Papers_Magister`, 2026-07-23): los baselines
de Pueyo 2024 son flooding, hop count, ETX y RSSI, así que el set queda alineado
con el paper ancla. SNR se excluye: en nuestro modelo de ruido es transformación
monótona del RSSI, rankings idénticos, las mismas rutas.

---

## 4. Tipos de corrida

| Tipo | Duración | Carga | Métricas | Guarda de tiempo |
|---|---|---|---|---|
| perf | 40 ks | 100 paq/par (carga Pueyo) | PDR, adm, fwd, latencia p50/p95, overhead | 1800 s |
| lifetime | **300 ks** | 1400 paq/par (sostenido) | FND, T50, cola SoC, desglose de energía | **5400 s** |

Dos lecciones incorporadas, las dos aprendidas a base de perder campañas:

- **300 ks, no 100 ks, para cualquier cosa que mida FND.** A 100 ks nadie muere
  y `fnd_s` sale −1 (centinela de "sin muertes") en el 100% de las celdas. El
  CSV sale con todas sus filas y nada delata que no midió nada.
- **La guarda de tiempo debe escalar con el horizonte.** Al pasar de 100 ks a
  300 ks sin tocar el `timeout` de 900 s, 244 de 320 celdas murieron con
  rc=124. Mismo síntoma: el CSV parece completo.

En ambos casos el chequeo que los habría cazado es el mismo — **contar filas con
rc=0, no filas totales** — y ahora está en todos los runners.

---

## 5. Bloques

### 5.1 Bloque principal: la matriz factorial (sustituye a E1, E2, E3)

La v2 medía métrica, MAC y escenario en bloques separados. Eso da efectos
principales pero no interacciones, y ya sabemos que hay al menos una (MAC×métrica,
negativa) y probablemente otra (métrica×cadencia, que es lo que E20 destapó).

| Bloque | Diseño | Celdas | Tipo |
|---|---|---|---|
| **M1 matriz de rendimiento** | {composite, toa_only, rssi} × {CSMA/CAD, ALOHA} × 3 escenarios × {duty 1%, off} × {SF7-12, SF7-8} × 8 N × 20 seeds | 11 520 | perf |
| **M2 matriz de vida útil** | {composite, toa_only} × 3 escenarios × {balizas 60, 300, 900, 1800 s} × N {25, 49, 100} × 20 seeds | 1 440 | lifetime |
| **M3 mecanismo** (demostración, no barrido) | δ ∈ {0, 0.25, 0.85} × 3 escenarios × {SF7-12, SF7-8} × N {25,49} × 20 — **ya corrido**: E19, 720 celdas | 0 nuevas | lifetime |

M1 absorbe E1, E2 y buena parte de E9. M2 absorbe E3 y E4 parcialmente, y es el
bloque que sostiene Q5.

### 5.2 Bloques que sobreviven de la v2

| Bloque | Diseño | Celdas | Estado |
|---|---|---|---|
| E4 duty (Q5 reformulada) | DC ∈ {off, 1, 2, 5, 10}% × grid-a2a × N {25,49} × 20 | 200 | pendiente; δ deja de ser factor, así que baja de 400 a 200 |
| E4b disciplinas | {time_off_air, sliding_window, fixed_window} × 4 seeds | 12 | pendiente (apéndice) |
| E6 baseline flooding | flooding × {grid-a2a, random-conv1} × 8 N × 20 | 320 | pendiente |
| E5 robustez | pesos α/β en random-49 (8 combos × 8 seeds) + bimodal | ~64 | pendiente |

**E4c (US915) — propuesta de eliminación.** Su justificación en la v2 era
triangular Q5: "sin duty, δ debería recuperar su efecto". δ no tiene efecto que
recuperar, ni con duty ni sin él (E17: cuota hasta 0.85; E19: tres topologías).
Se propone retirarlo y dedicar esas 40 celdas a M2. **Decisión para el G4.**

### 5.3 Campañas ya cerradas que entran al paper

| ID | Qué establece | Celdas |
|---|---|---|
| **E14 (m1)** | **desglose energético a margen 1 dB: reposo 46.46%, TX 33.71%, RX 18.01%, CAD 1.83%; el ruteo gobierna el 2.21%** | **320** |
| E15 | δ = 0 en las tres topologías | 360 |
| **E16 (m1)** | **frontera α/β a margen 1 dB: 85/15 gana vida útil (p<0.0001); la ventaja viene de tener β>0, no de ajustarlo** | **480** |
| E17 | δ hasta cuota 0.85 en el escenario de máximo relevo | 280 |
| E18 | δ sin lotería de batería, con FND medible | 320 |
| E19 | mecanismo de δ en 3 topologías × 2 rangos | 720 |
| **E20** | **frontera de cadencia de balizas, 60 → 14400 s** | **720** |
| **E7 (m1)** | **no hay umbral de SF: el colapso era nuestro. Dos canales, 640 celdas** | **640** |
| **E21b+c** | **réplica de Pueyo-Centelles: las ocho subfiguras (11a-d rejilla, 12a-d aleatoria) con margen 1 dB y los dos modelos de interferencia, 20 semillas** | **3 840** |
| E22 | precio de la baliza: óptimo interior en 18 rutas; su punto no es realizable | 300 |
| E23 | cuánta reserva necesita el selector de SF; la banda muerta de 240–251 m | 500 |
| E24 | las conclusiones sobreviven al margen: A intacto, B se atenúa | 320 |
| E25 | δ = 0 también con margen 1 dB — cierra el último caveat de δ | 240 |
| **E26** | **facturación plana de balizas: precio del supuesto de FLoRaMesh** | **240** |
| sondas | dominancia de relevo (72 escenarios), consumo vs carga | 75 |

**Nota sobre E21b y E26.** Las dos miden supuestos AJENOS, no nuestro protocolo.
E26 en particular activa `--pueyoFlatBeaconBytes=12`, que es físicamente
irrealizable y existe solo como instrumento. Cualquier figura que salga de esas
campañas debe declararlo en el pie. Ver §10.4.

**Deuda declarada de estas campañas:**
- E22 corrió a margen 0. El mecanismo del ToA no depende del margen; el óptimo
  interior sí es contraste pareado y hay que repetirlo con margen 1.
- **Sus figuras son BOXPLOTS y no publican valores numéricos.** Cualquier cifra
  "de Pueyo" es una lectura nuestra a ojo, no un dato. Retractado el 2026-08-07:
  los seis valores que veníamos usando como "publicados" de su fig. 11a no
  tenían fuente verificable -- aparecían solo en ficheros que habíamos escrito
  nosotros. **La comparación con su trabajo es de TENDENCIA, no de números**, y
  ninguna figura nuestra lleva ya una línea con sus valores.

**Coste restante estimado:** ~13 500 celdas (11 520 perf + ~2 000 lifetime).
Con el servidor a 14 hilos, del orden de 20–30 h de pared para perf y 15–20 h
para lifetime. Disco: 1–2 MB por corrida con la disciplina vigente.

---

## 6. Estadística declarada

Sin cambios de fondo respecto a la v2, con tres precisiones que la práctica
obligó a añadir:

- Comparaciones **pareadas por semilla** (mismo placement y tráfico, cambia solo
  el factor): mediana de diferencias pareadas como estimador principal, test de
  signos **excluyendo empates**, y cociente de medias reportado al lado.
- **El estimador se fija antes de mirar los datos.** Motivo medido: el mismo
  contraste da +119.3% por media de cocientes, +101.4% por mediana y +18.3% por
  cociente de medias. Elegir después es elegir el que favorece.
- **Corrección por comparaciones múltiples** (Bonferroni) cuando un bloque
  produce más de tres contrastes sobre la misma familia de métricas. Ya cambió
  una conclusión: el "óptimo" de 0.50/0.50 en E16 (p=0.017) no sobrevive al
  umbral de 0.00625 con 8 contrastes.
- **Validez de la celda antes que su valor**: se cuentan filas con rc=0, no
  filas totales, y se declara el recuento en cada tabla.
- Determinismo verificado; un tag de git por campaña; ningún conjunto del paper
  mezcla binarios.

---

## 7. Baselines de protocolo

Sin cambios respecto a la v2: **flooding SÍ** (E6), **AODV NO** (descubrimiento
prohibitivo bajo DC 1%), **ETX NO, citar a Pueyo**.

Se añade un baseline implícito que resultó ser informativo: `toa_only` con la
ecuación (4) de Pueyo-Centelles es, en canal determinista, **un protocolo de SF
fijo** — usa SF7 en el 99.4–100% de las transmisiones. Eso merece un párrafo
propio, porque explica sus resultados publicados mejor que cualquier
comparación agregada.

---

## 8. Gates

- **G1** ~~`dutyon8` completo y VALIDATION.md al día~~ **CERRADO 2026-07-23**
- **G2** ~~sonda de cada celda + `preflight.sh` verde~~ **CERRADO 2026-07-24**
- **G3** ~~servidor arriba, sincronizado, preflight~~ **CERRADO 2026-07-24**
  (10/10 suites, 9 perfiles; política de build: un árbol, asserts-on)
- **G3b** ~~auditoría de los 147 parámetros + arreglo de los ocho silenciosos~~
  **CERRADO 2026-08-03** (§10). Gate nuevo: no estaba en la v2 porque no
  sabíamos que hiciera falta. **Reabierto y vuelto a cerrar el 2026-08-06**: tres
  defectos más (capacidad de baliza, margen de SF, tags en la cola de salida)
  elevan la cuenta a **once**. El último, `ProcessTxQueue` borrando los packet
  tags, dejó correr una campaña entera de 120 celdas sin ruteo y con rc=0.
- **G3c** **replicación externa (nivel 3 de V&V)** — **CERRADO 2026-08-06 a
  177 m, PARCIAL a 248 m.** Gate nuevo: `PAPER_STRATEGY.md` §2 lo declaraba
  bloqueante de todo lo demás y lo era. Queda parcial porque las figuras 11c y 12
  no están digitalizadas, no por falta de cómputo.
- **G4** **este DoE aprobado por coautores y storyboard v1 acordado** —
  **ABIERTO**, es lo que este documento pide.

---

## 9. Decisiones

### 9.1 Resueltas en la v2 (2026-07-24), sin cambios

| # | Decisión | Resolución |
|---|---|---|
| D1 | flooding como baseline | **SÍ** → E6 |
| D2 | ETX: medir o citar | **citar a Pueyo** |
| D3 | rssi | **implementar clase, línea completa** |
| D4 | niveles del barrido DC% | **{off, 1, 2, 5, 10}** |
| D5 | k de multisink | **4** |

### 9.2 Abiertas, para el G4

| # | Decisión | Recomendación |
|---|---|---|
| **D6'** | E4c (US915): ¿se mantiene? | ⚠️ **RECOMENDACIÓN INVERTIDA 2026-08-08.** Decía "retirar" porque su premisa era que δ no tiene efecto en ningún régimen. **E27 refutó esa premisa**: sin duty δ mejora el FND un 13.41%. Ahora E4c es la **validación externa del hallazgo principal** — un régimen regulatorio real sin la restricción del 1%. **Mantener y promover.** ~300 celdas |
| **D7** | ¿el paper reporta la métrica en régimen de 60 s, de 900 s, o ambos? | **ambos**, y explicar por qué difieren: ese contraste *es* el argumento cross-layer |
| **D8** | ¿se quita el byte de SoC de la baliza? | **no eliminarlo; medirlo** como variante `pueyo6b`. Ahorra 1.3–2.7% de la energía total — más que el techo de δ — pero rompe comparabilidad de formato con Pueyo |
| **D9** | ¿anuncio de rutas por evento en vez de por reloj? | **sí como trabajo futuro**, no como implementación para este paper. E20 lo motiva pero no lo prueba: la topología es estática |
| **D10** | ¿receptor con ciclo de trabajo? | **trabajo futuro declarado.** Es el 45.7% del gasto y hoy no existe knob; el radio nunca duerme |

---

## 10. Tabla de parámetros fijos

Valores efectivos leídos del binario auditado (`mesh_dv_effective_config.csv`,
perfil `proposal_pueyo_like_csmacad`), no de la documentación. **Cada corrida
del paper emite este fichero y cada celda lo archiva**, de modo que cualquier
figura puede rastrearse hasta la configuración que la produjo.

### 10.1 Constantes de todas las campañas

| Grupo | Parámetro | Valor | Origen |
|---|---|---|---|
| **PHY** | modelo de propagación | FLoRa urbano, n=2.08, L(40 m)=127.41 dB | perfil |
| | potencia TX | 20 dBm | perfil (congelado) |
| | sensibilidades SX1276 | −124/−127/−130/−133/−135/−137 dBm | SF7…SF12 |
| | alcances derivados | SF7 251.0 m … SF12 1058.4 m | calculados |
| | ToA | Semtech AN1200.13 | — |
| | preámbulo | 16 símbolos | perfil (congelado) |
| **Canal** | sombreado | `static` — una muestra log-normal **por enlace**, recíproca | `--shadowingModel` |
| | σ | 3.57 dB | `--shadowingSigmaDb` (con puerta) |
| **MAC** | duty | 1%, ventana 3600 s, `time_off_air` | `--allowDutyOverride` |
| **Ruteo** | formato de baliza | `pueyo7b` (7 B por entrada, 1 de ellos SoC) | perfil (congelado) |
| | codificación de coste | `cost255`, paso 0.025, recorte [1,255] | perfil |
| | histéresis de conmutación | activada, umbral 0 | `--routeSwitchHysteresis` |
| | tabla DV | 2 rutas/destino, 1024 totales | perfil (congelado) |
| | política de anuncio | `cost_weighted` | perfil (congelado) |
| | payload DV | 251 B | perfil (congelado) |
| | modo de enlace SF | `deterministic_sensitivity` | perfil (congelado) |
| **Energía** | capacidad | ≈266 mAh (implícita) | `BatteryFullCapacityJ` |
| | reposo | corriente constante, **el radio nunca duerme** | modelo |
| | rampa Ψ | b_lo=0.20, b_hi=0.50, exponente 2.0 | inerte con δ=0 |
| **Métrica** | pesos | **α=0.85, β=0.15, δ=0** | E16, E17, E18, E19 |

### 10.2 Factores (lo que se varía, y con qué palanca)

| Factor | Niveles | Palanca **verificada** | Palanca que **no** funciona |
|---|---|---|---|
| métrica | composite / toa_only / rssi / hops | `--routeMetricMode` | — |
| pesos | α, β, δ | `--compositeW{Toa,Hop,Energy}` | — |
| MAC | CSMA/CAD, ALOHA | **el perfil** | `--enableCsma` (se descartaba) |
| duty on/off | 1%, off | presencia de `--allowDutyOverride=true` | `--enableDuty` (se descartaba) |
| nivel de duty | 1, 2, 5, 10% | `--dutyOverridePct` | `--dutyLimit` (se descartaba) |
| rango de SF | 7-12, 7-8 | `--allowPaperLikeSfRangeVariant` + `sfMin`/`sfMax` | sin la puerta **aborta** (correcto) |
| cadencia de balizas | 60…1800 s | `--allowBeaconOverride` + `beaconInterval*` | — |
| escenario | a2a / conv1 / msink4 | `nodePlacementMode` + `trafficMode` / `forcedDataDestinationId` / `numSinks` | — |
| N | 9…100 | `--nEd` (cuadrado perfecto) | — |
| batería inicial | U[60,100]% o fija | `--socInitMin` / `--socInitMax` | — |

Las tres palancas de la columna derecha se aceptaban y se descartaban en
silencio hasta el 2026-08-03. **Hoy abortan** con el mensaje exacto de qué se
pidió y qué se aplicó. Ninguna campaña las había usado — se verificó con un
`grep` sobre los 19 runners.

### 10.3 Resultado de la auditoría del instrumento

De los 147 parámetros expuestos por línea de comandos:

| veredicto | n | significado |
|---|---:|---|
| aplican y cambian la simulación | 61 | verificado por huella md5 |
| se protegen abortando | 36 | el perfil los congela y el binario lo dice |
| inertes con causa buena | 49 | fuera de régimen, meta-flags, `no-op` declarados |
| **cuelgan el motor** | **1** | `studySuperframeEnable`: periodo 1 s < ToA SF12 2.4 s. Latente, ninguna campaña lo usa |

Los 107 atributos cableados con `SetDefaultFailSafe` existen todos (96 de
`dv-cl`, 11 de `lorawan`): no hay cableado que se descarte por nombre erróneo.

**Actualización 2026-08-06 — tres defectos más, y el patrón que los une.** La
auditoría de los 147 parámetros era necesaria pero no suficiente: encontró los
que se aceptaban y no se aplicaban, no los que se aplicaban mal.

| # | defecto | por qué no se veía |
|---|---|---|
| 9 | `GetBeaconRouteCapacity` salía por un return temprano si `dvPayloadMaxBytes>0`, y el perfil comparable lo fija en 251 | `--dvBeaconMaxRoutes` era código muerto: K=0,1,4,16 daban corridas **bit-idénticas** |
| 10 | `sfLinkMarginDb=0` deja al selector eligiendo el SF más rápido que *teóricamente* alcanza, sin reserva | a 240–250 m el enlace queda con 0.04–0.40 dB y la red entrega **cero** con rc=0 y sin un error. Y en nuestro propio punto de operación deprimía el PDR ×2.3 |
| 11 | `ProcessTxQueue` hace `RemoveAllPacketTags()` y repone solo el metric tag | el tag de rutas moría en la cola de salida. La primera pasada de E26 corrió entera con hops=0.0000 en 120 celdas y PDR 0.21, superando su propia guarda |

**Lección de método, aplicable a toda campaña futura.** Cada campaña lleva una
precondición que verifica que el factor MUERDE antes de gastar una celda. El
defecto 11 enseñó la parte que faltaba: **el invariante de esa precondición no
puede ser la métrica que la campaña mide.** En E26 la guarda pedía PDR>0.05 y el
PDR era justo lo que la campaña variaba a propósito; el invariante correcto eran
los saltos, que deben coincidir entre brazos. Y debe ser **de un solo lado**
cuando el tratamiento puede moverlo legítimamente: en E26 los saltos suben hasta
un 12.9% a 248 m porque las balizas baratas mejoran la convergencia, y eso es el
efecto, no un fallo.

### 10.4 Convención de pie de figura

Toda figura del paper declara **caso** y **variable**:

> `[caso: malla SF7-8, duty 1%, CSMA/CAD, balizas 900 s, N=49, 20 semillas]`
> `varía: métrica de ruteo`

Lo que no aparezca en el corchete está en §10.1. Cualquier desviación respecto a
§10.1 se declara en el corchete, no se omite.

**Dos reglas añadidas el 2026-08-06:**

1. **Toda figura es un boxplot sobre semillas**, no una línea de medias. Las
   medias ocultan la dispersión y en varias campañas la dispersión entre semillas
   es del orden del efecto que se reporta. Media y n van en el pie.
2. **Las figuras de supuestos ajenos se marcan como tales.** Cualquier figura que
   use `--pueyoFlatBeaconBytes` o `--interferenceModel=pueyo_fixed_capture`
   describe el modelo de OTRO grupo, no el nuestro, y el pie lo dice:

> `[EMULACIÓN de supuestos de FLoRaMesh: SF perfectamente ortogonales +`
> `facturación de balizas a 12 B. NO es el comportamiento de DV-CL.]`

---

## 11. Storyboard v1 propuesto

Cambios respecto al v0: **fuera F3, F6, F7 y F9** (las cuatro dependían de que δ
funcionara o de una sinergia que no existe); **dentro tres figuras nuevas**, una
de ellas como figura principal.

| Fig | Contenido | Pregunta | Fuente | Estado |
|---|---|---|---|---|
| F1 | stack DV-CL (protocolo, métrica, MAC, seams) | — | doc módulo | hecho |
| F2 | presupuesto energético por actividad, con el techo del ruteo marcado | Q2 | E14 | **datos listos** |
| **F3'** | **frontera de cadencia de balizas: FND, T50 y PDR vs cadencia** | **Q5** | **E20** | **datos listos — figura principal** |
| F4 | PDR vs N por métrica × escenario (incl. flooding) | Q1 | M1, E6 | pendiente |
| F5 | FND/T50 vs N por métrica × cadencia | Q1, Q5 | M2 | pendiente |
| **F6'** | **la ventaja de la métrica según la cadencia: cómo el canje se degrada 7.6×** | Q1, Q5 | E20 | **datos listos** |
| **F7'** | **mecanismo de δ: reparte la carga (−22% CV) y aun así acorta la vida** | Q3 | E17–E19 | **datos listos** |
| F8 | Δ-pareadas resumen (forest plot) | Q1–Q6 | todos | pendiente |
| **F9'** | **~~umbral de SF~~ → el sesgo de la ec. (4) hacia SF bajos SIN colapso: `toa_only` usa 99.7% de SF7 a 177 m frente al 23.2% de la compuesta** | Q6 | E7 (m1) | **datos listos, claim reescrito** |
| T1 | comparación flooding | Q1 | E6 | pendiente |
| T2 | disciplinas de duty (apéndice) | — | E4b | pendiente |
| **T3** | **tabla de parámetros fijos** (§10.1) | — | — | **hecha** |

Cinco de las once tienen ya sus datos. Las pendientes dependen de M1, M2 y E6,
que son el gasto que este DoE pide autorizar.

---

## 12. Qué se pide al G4

> **El paquete completo está en `G4.md`**, con orden del día, riesgos declarados
> y las cinco decisiones desarrolladas. Esto es el resumen.

1. **D1 — aprobar el cambio de tesis.** Es lo único que si no se aprueba invalida
   el resto. El paper pasa de *"el término de energía no sirve"* a **"la
   regulación anula un término que funciona sin ella"** (E27: el signo del
   contraste cambia entre regímenes, 30/30 semillas en cada brazo).
2. **D2 — el régimen principal.** ¿Duty al 1% con el otro como contraste, o el
   duty como factor de primer nivel? La tesis nueva apunta a lo segundo y duplica
   las figuras.
3. **D3 — FND o T50 como métrica primaria.** E27 obliga a elegir: δ sube el FND
   un 13.41% y baja el T50 un 6.70%. Recomendación: reportar ambas y explicar el
   intercambio, porque nadie en la literatura las distingue.
4. **D4 — E4c (US915) revive**, con la recomendación invertida (§9.2).
5. **D5 — ¿se publica la retractación del umbral de SF?** La crítica que le
   hacíamos a Pueyo era un artefacto de nuestro propio selector.
6. **Autorizar M1, M2 y E4c** (~13 800 celdas, 37–53 h).

Todo lo que se podía cerrar con evidencia ya está cerrado y documentado arriba;
lo que queda son decisiones de coautoría.
