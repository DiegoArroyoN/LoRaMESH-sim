# DOE.md — Diseño de experimentos (F3.1), borrador para coautores

**Estado: BORRADOR v1, 2026-07-23.** Para revisión con S. Sobarzo y G. Saavedra.
Las métricas se definen en `METRICS.md` (F3.2); la evidencia citada está en
`VALIDATION.md` con sus datasets versionados en `tools/validation/`.

## 1. Preguntas de investigación e hipótesis

**Los claims se derivan de los datos, no al revés (decisión de Diego,
2026-07-24).** Las campañas completas y todos los escenarios se corren primero;
las conclusiones del paper se escriben después, sobre esa evidencia. Lo que
sigue son las preguntas que el DoE debe poder responder y las hipótesis con su
evidencia **preliminar** (medida en régimen parcial, a re-confirmar en el
régimen final antes de afirmarse).

| # | Pregunta / hipótesis | Bloque | Evidencia preliminar |
|---|---|---|---|
| Q1 | ¿La métrica compuesta cross-layer mejora PDR/vida útil frente a métricas de una capa (ToA, hops, RSSI) bajo duty EU868 1%? | E1, E3 | sin medir en régimen principal |
| Q2 | ¿Cómo actúa el término δ·Ψ sobre la energía? Hipótesis: redistribuye la **cola** del SoC (no la media) y satura en δ≈0.10 (interruptor, no perilla) | E4, E5 | sostenida **sin** duty (VALIDATION 2026-07-22 d/e/g); ver Q5 |
| Q3 | ¿Qué fracción del presupuesto energético gobierna el ruteo? Hipótesis: ~1.4% con receptores siempre encendidos (techo de cualquier métrica energy-aware) | contexto | medido sin duty; recalcular duty-on |
| Q4 | ¿Hay sinergia MAC×routing? ¿El beneficio de la métrica depende del MAC? (término de interacción del 2×2 ALOHA/CSMA × ToA/compuesta) | E2 | sin medir |
| Q5 | ¿El duty cycle actúa como **ecualizador implícito de energía**, acotando la desigualdad de airtime que δ·Ψ explota? | E4, E4c | sostenida: FND +0.04% duty-on vs +9.69% sin duty (colapso 240×), canal cambia a PDR +0.52% (`dutyon8`, 8/8 pares, VALIDATION 2026-07-23) |

Q5 es la hipótesis más citable si se confirma: Cotrim & Margi (2024) estudian el
DC como cuello de botella de throughput/latencia; nadie lo ha caracterizado como
ecualizador del gasto que anula el margen del ruteo energy-aware. El barrido DC%
(E4) traza la transición entre regímenes y US915 (E4c, sin duty) triangula la
generalidad: allí δ debería recuperar su efecto.

## 2. Régimen principal declarado

**Duty-on 1% por nodo, disciplina `time_off_air` (ETSI).** Es lo que el
proyecto de tesis declara como condición obligatoria de todos los escenarios, y
lo que hace la literatura seria (Pueyo lo fuerza en FLoRaMesh; Mugerwa y Chen
lo aplican como restricción; Udugampola lo ignora pero lo justifica por operar
en túneles). Duty-off queda solo como (a) apéndice de comparabilidad con Pueyo
2024 y (b) punto extremo del barrido DC% de C5.

Consecuencia asumida: **todos los resultados previos a 2026-07-23 son duty-off**
y sirven como caracterización del régimen sin restricción, no como resultados
del régimen principal. E1–E3 los re-establecen bajo duty-on.

## 3. Factores y niveles

| Factor | Niveles | Nota |
|---|---|---|
| Métrica | composite / toa_only / hops (α=0,β=1,δ=0) / rssi | hops y toa por atributos, costo cero; **rssi = clase nueva sobre `DvClRoutingMetric` (a implementar), línea completa en E1** |
| MAC | CSMA/CAD / ALOHA | perfiles existentes, ALOHA validado contra S=G·e^(−2G) |
| Duty | 1% (principal) / off / DC% ∈ {2,5,10} solo en E4 | `--allowDutyOverride`, `--dutyOverridePct` |
| Escenario | grid×all-to-all / random×convergecast-1-sink / random×multisink-k (rec: k=4) | plumbing existente; sonda por celda antes de lanzar (gate G2) |
| N | 9, 16, 25, 36, 49, 64, 81, 100 | Pueyo llega a 64; lo excedemos |
| Semillas | 20 (rngRun 1–20), **pareadas por semilla** | moda de la literatura rigurosa (JMAC 20, LB-OLSR 20, Serati 25); Pueyo no reporta semillas |
| Batería inicial | U[60,100]% (estándar); bimodal solo en robustez | |

Sustento de literatura (NotebookLM `Papers_Magister`, 2026-07-23): los
baselines del propio Pueyo 2024 son flooding, hop count, ETX y RSSI, así que
nuestro set de métricas queda alineado con el paper ancla. SNR se excluye con
justificación: en nuestro modelo de ruido es transformación monótona del RSSI,
rankings idénticos, las mismas rutas (una línea en el paper).

## 4. Tipos de corrida

| Tipo | Duración | Carga | Métricas | Costo aprox |
|---|---|---|---|---|
| perf | 40 ks (generación 100 paq/par ≈ 24 ks + 16 ks de drenaje; bajo duty el drenaje es lento y `queued_at_stop` se contabiliza) | 100 paq/par (carga Pueyo) | PDR, adm, fwd, latencia p50/p95, overhead | ~1 min (N=25) a ~20 min (N=100) |
| lifetime | 300 ks | 1400 paq/par (tráfico sostenido toda la corrida) | FND, T50, cola SoC (min, p10), desglose energía | ~10 min (N=25) a ~3 h (N=100) |

Lección incorporada: con 100 paq/par los datos se agotan en el 8% de una
corrida de 300 ks y el FND mide balizado en red vacía (VALIDATION 2026-07-22 b).

## 5. Bloques

| Bloque | Diseño | Corridas | Tipo |
|---|---|---|---|
| E1 ranking de métricas | {composite, toa, hops, **rssi**} × 3 escenarios × 8 N × 20 seeds, duty-on, CSMA/CAD (rssi absorbe la antigua E1b) | 1920 | perf |
| E2 interacción 2×2 | ALOHA × {composite, toa} × 3 esc × 8 N × 20 (celdas CSMA reusadas de E1); sinergia = Δpareada de diferencias | 960 | perf |
| E3 vida útil | {composite, toa} × {grid-a2a, random-conv1} × N {25,49,100} × 20, duty-on | 240 | lifetime |
| E4 duty (Q5) | DC ∈ {off, 1, 2, 5, 10}% × {δ=0.25, δ=0} × grid-a2a × N {25, 49} × 20 | ≤400 | lifetime |
| E4b disciplinas | {time_off_air, sliding_window, fixed_window} × 1 config × 4 seeds (apéndice metodológico) | 12 | perf |
| E4c US915 | perfil US915 (sin duty, dwell 400 ms) × {δ=0.25, δ=0} × grid-a2a × N 25 × 20 — triangulación de Q5 | 40 | lifetime |
| E6 baseline flooding | flooding gestionado × {grid-a2a, random-conv1} × 8 N × 20, duty-on | 320 | perf |
| E5 robustez | pesos α/β/δ en random-49 duty-on (8 combos × 8 seeds); interferencia goursaud/pueyo (hecho); bimodal (replicar duty-on solo si Q5 lo pide) | ~64 | lifetime |

Totales: ~3 250 perf + ~750 lifetime. Estimación: 400–600 h-core perf,
700–1000 h-core lifetime. En un servidor de 16–32 cores: 1–2 semanas de pared.
Disco: con la disciplina vigente (sin pcap, sin logs verbosos, borrar CSVs
pesados tras resumir) cada corrida queda en 1–2 MB, el programa completo <20 GB.
Diego autorizó borrar campañas antiguas (2026-07-23).

## 6. Estadística declarada

- Comparaciones **pareadas por semilla** (mismo placement y tráfico, cambia solo
  el factor): media de diferencias con IC95 y test de signos como respaldo no
  paramétrico. Motivo medido: la varianza entre semillas (PDR 0.18–0.22) aplasta
  los efectos entre configuraciones (Δ≈0.002); un boxplot los esconde.
- Niveles absolutos: media ± IC95 vs N (boxplots solo en N selectos).
- 20 semillas: potencia sobrada según efectos observados (t=16 con n=8).
- Determinismo verificado (3/3 bit-idéntico tras refactor); datasets versionados.
- Ningún conjunto del paper mezcla binarios: un tag de git por campaña.

## 7. Baselines de protocolo

- **Flooding gestionado: SÍ** (decidido 2026-07-24). Lo usan Pueyo, Udugampola y
  Joy & Branch; es el estándar industrial de facto (Meshtastic). Implementación
  sobre el seam existente → bloque E6.
- **AODV: NO** (decidido), con justificación citable: el descubrimiento de rutas
  bajo DC 1% es prohibitivo en airtime (Hong 2022 lo usa, pero a 1000 nodos y sin
  DC explícito). Párrafo en related work.
- **ETX: NO implementar, citar a Pueyo** (decidido). Pueyo ya lo comparó contra
  ToA; reimplementarlo exige un estimador de PRR por enlace (más código y ruido)
  que no aporta al hilo cross-layer.

## 8. Gates antes de lanzar campañas

- **G1**: ~~`dutyon8` completo y VALIDATION.md actualizado~~ **CERRADO 2026-07-23**
  (8/8 pares, `tools/validation/dutyon_delta_isolation.csv`).
- **G2**: ~~sonda de cada celda de escenario + `preflight.sh` verde~~ **CERRADO
  2026-07-24** en local: las 3 celdas producen tráfico y entregas con la forma
  correcta (all-to-all→25 destinos, convergecast→1, multisink→4), guard sin
  abortar; preflight verde en WSL.
- **G3**: ~~servidor ns3-remote arriba, sincronizado, preflight~~ **CERRADO
  2026-07-24. PREFLIGHT OK en el servidor** (10/10 suites, 9 perfiles,
  agotamiento fnd=5270, guard). Cadena de tres fallos, ninguno el servidor
  "caído": (1) multiplexor SSH zombi en el cliente Windows (arreglado en
  `~/.ssh/config`); (2) `ns346/ns-3-dev` (ns-3.46) sin el módulo `lorawan`,
  instalado el de 3.46; (3) parche de higiene en `lorawan` + dejarlo
  library-only (sus ejemplos/tests traen agregadores que la guarda de 3.46
  rechaza). Todo reproducible en `tools/validation/server_setup_lorawan.sh`.
  **Política de build decidida (2026-07-24): un solo árbol, asserts-on por
  defecto** — el determinismo bit-idéntico ya se verificó en WSL, y en un
  servidor de 16 cores el sobrecoste de los asserts es tolerable frente al valor
  de que el preflight y las campañas corran sobre el mismo binario que aborta
  ante un bug. Si una campaña grande resulta demasiado lenta, se reconfigura a
  release para ESA campaña y se re-verifica determinismo contra un punto
  asserts-on. Disco holgado (156 GB libres): no se limpió nada.
- **G4**: este DoE aprobado por coautores y storyboard v1 acordado.

## 9. Decisiones (resueltas 2026-07-24)

| # | Decisión | Resolución |
|---|---|---|
| D1 | flooding como baseline | **SÍ** → E6 |
| D2 | ETX: medir o citar a Pueyo | **citar a Pueyo** (no implementar) |
| D3 | rssi | **implementar clase, línea completa en E1** (equivalencia≈ToA esperada bajo SF-por-sensibilidad ES un resultado: ToA precia el recurso regulado, RSSI no) |
| D4 | niveles del barrido DC% | **{off, 1, 2, 5, 10}** |
| D5 | k de multisink | 4, colocados por `numSinks` (default salvo aviso) |
| D6 | subsección US915 | **SÍ** → E4c (~40 corridas) |

## 10. Storyboard v0 (para discutir, no congelado)

| Fig | Contenido | Pregunta | Fuente |
|---|---|---|---|
| F1 | stack DV-CL (protocolo, métrica, MAC, seams) | — | hecho (doc módulo) |
| F2 | presupuesto energético por actividad | Q3 | rehacer duty-on |
| F3 | mecanismo: cola del SoC (min, p10) con y sin δ | Q2 | hecho duty-off; añadir duty-on |
| F4 | PDR vs N por métrica × escenario (incl. flooding) | Q1 | E1, E6 |
| F5 | FND/T50 vs N por métrica | Q1 | E3 |
| F6 | interacción 2×2 MAC×métrica (sinergia pareada) | Q4 | E2 |
| **F7** | **ganancia de FND por δ vs DC% (la transición del ecualizador) + punto US915** | **Q5** | **E4, E4c** |
| F8 | Δ-pareadas resumen (forest plot) | Q1–Q5 | todos |
| F9 | sensibilidad de pesos (meseta de δ) | Q2 | hecho duty-off; E5 |
| T1 | comparación flooding | Q1 | E6 |
| T2 | disciplinas de duty (apéndice metodológico) | — | E4b |
