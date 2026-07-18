# METRICS.md — diccionario de métricas (F3.2)

Definición unívoca de cada métrica usada en campañas y paper. Los
scripts de análisis y el colector del simulador deben referirse a estas
definiciones; cualquier cambio se versiona aquí.

| Métrica | Columna | Definición | Unidad |
|---|---|---|---|
| PDR end-to-end | `pdr` | entregados / **generados** (desde la generación en el origen; los no admitidos y descartes de cola cuentan como pérdida) | fracción [0,1] |
| Ratio de admisión | `adm` | paquetes con primera Tx en el origen / generados (`source_first_tx_count / total_data_generated`) | fracción |
| Éxito de reenvío | `fwd` | entregados / admitidos (`delivered_per_tx_attempt`) | fracción |
| Vida útil T50 | `t50` | instante en que el 50% de los nodos agota su batería (muerte por energía) | s |
| Primera muerte (FND) | `fnd` | instante de la primera muerte de nodo | s |
| Batería mínima restante | `minrem` | fracción de energía restante del nodo más agotado al fin de la corrida | fracción |
| Batería máxima restante | `maxrem` | ídem, nodo menos agotado; `maxrem−minrem` = dispersión de carga (balanceo) | fracción |
| Energía usada | `used_j` | energía total consumida por la red | J |
| Latencia p50/p95 | `delay_p50`, `delay_p95` | percentiles del retardo entrega−generación (solo entregados) | s |
| Saltos promedio | `avg_hops` | promedio de saltos de los paquetes entregados | saltos |
| Overhead de control | `oh_ratio` | bytes de control / bytes de datos entregados | ratio |
| Control/datos Tx | `ctrl_data_ratio` | transmisiones de control / transmisiones de datos | ratio |
| Reenvíos totales | `fwd_tx_total` | transmisiones de reenvío realizadas por relays | conteo |

Descomposición de pérdidas (presupuesto, suma 100% de los generados):
`PDR + pérdida de reenvío (adm − pdr) + no admitido (1 − adm)`; bajo la
ablación de regla DC, el "no admitido" se separa en
**duty-induced** = adm(sin DC) − adm(DC) y **residual de energía/cola**
= 1 − adm(sin DC).

Convenciones: SI en todo; semillas vía `RngRun` incremental (1..N);
promedios sobre semillas con IC 95% (Welch); comparaciones multi-celda
con corrección Holm-Bonferroni.
