# DOE.md — Diseño de experimentos (F3.1), borrador para coautores

**Estado: BORRADOR v1, 2026-07-23.** Para revisión con S. Sobarzo y G. Saavedra.
Las métricas se definen en `METRICS.md` (F3.2); la evidencia citada está en
`VALIDATION.md` con sus datasets versionados en `tools/validation/`.

## 1. Claims que el paper debe sostener

| # | Claim | Bloque | Estado |
|---|---|---|---|
| C1 | La métrica compuesta cross-layer mejora PDR/vida útil frente a métricas de una capa (ToA, hops, RSSI) bajo duty cycle EU868 1% | E1, E3 | por medir en régimen principal |
| C2 | El término de energía δ·Ψ redistribuye la **cola** del SoC (no la media) y compra vida útil; el efecto satura en δ≈0.10 (interruptor, no perilla) | E4, E5 | demostrado **sin** duty (VALIDATION 2026-07-22 d/e/g); ver C5 |
| C3 | En receptores siempre encendidos el ruteo solo gobierna ~1.4% del presupuesto energético: techo de cualquier métrica energy-aware | contexto | medido sin duty; recalcular bajo duty-on |
| C4 | Sinergia MAC×routing: el beneficio de la métrica compuesta depende del MAC (término de interacción del 2×2) | E2 | hipótesis; el 2×2 ya está declarado en el proyecto de tesis (DV-ToA/DV-CMP × ALOHA/CSMA) |
| C5 | **El duty cycle 1% actúa como ecualizador implícito de energía**: acota la desigualdad de airtime que el ruteo energy-aware explota. δ·Ψ es dependiente del régimen: FND +0.04% bajo DC 1% vs +9.69% sin DC (colapso 240×), y el canal del beneficio cambia a PDR (+0.52%, t=4.4) | E4 | **medido** (`dutyon8`, 8/8 pares, VALIDATION 2026-07-23) |

C5 es el hallazgo nuevo y el más citable: Cotrim & Margi (2024) estudian el DC
como cuello de botella de throughput/latencia en multihop; nadie lo ha
caracterizado como ecualizador del gasto que anula el margen del ruteo
energy-aware. El barrido DC% traza la transición entre regímenes, y US915
(dwell time, sin duty) da la validación de generalidad: allí δ debe recuperar
su efecto.

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
| Métrica | composite / toa_only / hops (α=0,β=1,δ=0) / rssi | hops y toa por atributos, costo cero; rssi = clase pequeña sobre `DvClRoutingMetric` |
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
| E1 ranking de métricas | {composite, toa, hops} × 3 escenarios × 8 N × 20 seeds, duty-on, CSMA/CAD | 1440 | perf |
| E1b rssi (alcance acotado) | rssi × 2 escenarios × N {9,25,49,100} × 20 | 160 | perf |
| E2 interacción 2×2 | ALOHA × {composite, toa} × 3 esc × 8 N × 20 (celdas CSMA reusadas de E1); sinergia = Δpareada de diferencias | 960 | perf |
| E3 vida útil | {composite, toa} × {grid-a2a, random-conv1} × N {25,49,100} × 20, duty-on | 240 | lifetime |
| E4 duty (C5) | DC ∈ {off, 1, 2, 5, 10}% × {δ=0.25, δ=0} × grid-a2a × N {25, 49} × 20 (N=49 con 10 seeds si aprieta) | ≤400 | lifetime |
| E4b disciplinas | {time_off_air, sliding_window, fixed_window} × 1 config × 4 seeds (apéndice metodológico) | 12 | perf |
| E4c US915 | perfil US915 (sin duty, dwell 400 ms) × {δ=0.25, δ=0} × grid-a2a × N 25 × 20 — triangulación de C5 | 40 | lifetime |
| E5 robustez | pesos α/β/δ en random-49 duty-on (8 combos × 8 seeds); interferencia goursaud/pueyo (hecho); bimodal (replicar duty-on solo si C5 lo pide) | ~64 | lifetime |

Totales: ~2 600 perf + ~750 lifetime. Estimación: 400–600 h-core perf,
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

- **Flooding gestionado: sí** (recomendado). Lo usan Pueyo, Udugampola y
  Joy & Branch; es el estándar industrial de facto (Meshtastic). Implementación
  sobre el seam existente.
- **AODV: no**, con justificación citable: el descubrimiento de rutas bajo DC
  1% es prohibitivo en airtime (Hong 2022 lo usa, pero a 1000 nodos y sin DC
  explícito). Párrafo en related work.
- **ETX: decisión abierta** (D2). Pueyo ya lo comparó contra ToA; se puede citar
  en vez de reimplementar. Implementarlo exige estimador de PRR por enlace.

## 8. Gates antes de lanzar campañas

- **G1**: ~~`dutyon8` completo y VALIDATION.md actualizado~~ **CERRADO 2026-07-23**
  (8/8 pares, `tools/validation/dutyon_delta_isolation.csv`).
- **G2**: ~~sonda de cada celda de escenario + `preflight.sh` verde~~ **CERRADO
  2026-07-24** en local: las 3 celdas producen tráfico y entregas con la forma
  correcta (all-to-all→25 destinos, convergecast→1, multisink→4), guard sin
  abortar; preflight verde en WSL.
- **G3**: servidor ns3-remote — **conexión resuelta 2026-07-24** (era un
  multiplexor SSH zombi en el cliente Windows, no el servidor; ver `~/.ssh/config`).
  Rama `dvcl-module-scaffold` empujada por SSH (HEAD cd7bb0b). **En curso**: el
  árbol `ns346/ns-3-dev` carecía del módulo `lorawan`; copiado desde el frozen y
  recompilando. Disco holgado (156 GB libres, 31%): no hace falta limpiar.
  Pendiente tras build: preflight en el servidor, y decidir política de dos
  configs (asserts-on para preflight / release para campañas).
- **G4**: este DoE aprobado por coautores y storyboard v1 acordado.

## 9. Decisiones abiertas para coautores

| # | Decisión | Recomendación |
|---|---|---|
| D1 | flooding como baseline | sí |
| D2 | ETX: medir o citar a Pueyo | citar; implementar solo si un revisor lo exigiera |
| D3 | alcance de rssi | acotado (E1b); esperada equivalencia≈ToA bajo SF-por-sensibilidad, y eso es un resultado: ToA precia el recurso regulado, RSSI no |
| D4 | niveles del barrido DC% | {off, 1, 2, 5, 10} |
| D5 | k de multisink | 4, colocados por `numSinks` |
| D6 | subsección US915 | sí: triangula C5 con 40 corridas |

## 10. Storyboard v0 (para discutir, no congelado)

| Fig | Contenido | Claim | Fuente |
|---|---|---|---|
| F1 | stack DV-CL (protocolo, métrica, MAC, seams) | — | hecho (doc módulo) |
| F2 | presupuesto energético por actividad | C3 | rehacer duty-on |
| F3 | mecanismo: cola del SoC (min, p10) con y sin δ | C2 | hecho duty-off; añadir duty-on |
| F4 | PDR vs N por métrica × escenario | C1 | E1 |
| F5 | FND/T50 vs N por métrica | C1 | E3 |
| F6 | interacción 2×2 MAC×métrica (sinergia pareada) | C4 | E2 |
| **F7** | **ganancia de FND por δ vs DC% (la transición del ecualizador) + punto US915** | **C5** | **E4, E4c** |
| F8 | Δ-pareadas resumen (forest plot) | C1–C5 | todos |
| F9 | sensibilidad de pesos (meseta de δ) | C2 | hecho duty-off; E5 |
| T1 | comparación flooding | C1 | D1 |
| T2 | disciplinas de duty (apéndice metodológico) | — | E4b |
