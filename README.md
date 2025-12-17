# Simulador LoRa Mesh sobre ns-3

Proyecto basado en ns-3 (3.45) que implementa una red LoRa en topología mesh con enrutamiento por Vector de Distancia, ADR hop-by-hop, MAC CSMA/CAD con duty-cycle y modelo de energía. Incluye scripts de simulación (`scratch/LoRaMESH-sim`) y un pipeline de análisis en `analysis/`.

## Estructura del repositorio
- `src/loramesh/`: módulo reusable de LoRaMesh.
  - `model/`: componentes principales (métrica compuesta, ADR, MAC CSMA/CAD+duty, modelo de energía, routing DV).
  - `helper/loramesh-helper.{h,cc}`: instala nodos, dispositivos y aplicaciones en un escenario lineal.
- `scratch/LoRaMESH-sim/`: simulador LoRaMesh listo para ejecutar.
  - `mesh_dv_baseline.cc`: `main` con CLI.
  - `mesh_dv_app.{h,cc}`: aplicación mesh (DV, forwarding, generación de tráfico).
  - `mesh_lora_net_device.{h,cc}` y cabeceras de MAC/metric tags.
  - `metrics_collector.{h,cc}`: recolector y exportador de CSV (TX, RX, rutas, delay, duty, energía, overhead).
- `analysis/`: análisis offline.
  - `run_analysis.py`: lee los CSV, deduplica TX/RX para PDR, y genera tablas y figuras.
  - `results/<label>/`: salidas de análisis (summary, CSV derivados, figuras).
- `build/`, `cmake-cache/`: artefactos de compilación (ignorados).

## Simulación: parámetros y ejecución
Script principal: `scratch/LoRaMESH-sim/mesh_dv_baseline`.

Parámetros CLI relevantes:
- `--nEd`: número de end devices (se crea un GW adicional).
- `--stopSec`: tiempo de simulación en segundos.
- `--spacing`: separación lineal entre nodos (m).
- `--gwHeight`: altura del gateway (m).
- `--enableAdr`: habilita ADR hop-by-hop.
- `--enableDuty`: aplica límite de duty-cycle.
- `--dutyLimit`: límite de duty (0.01 = 1%).
- `--enablePcap`: genera PCAP por nodo.

Ejemplos:
```bash
# Simulación base (sin ADR ni duty)
./ns3 run "scratch/LoRaMESH-sim/mesh_dv_baseline --nEd=5 --stopSec=120 --enableAdr=false --enableDuty=false"

# Con ADR y duty-cycle al 1%
./ns3 run "scratch/LoRaMESH-sim/mesh_dv_baseline --nEd=10 --stopSec=300 --enableAdr=true --enableDuty=true --dutyLimit=0.01"
```

Topología: lineal, nodos separados por `spacing`, gateway al final con altura `gwHeight`. El helper configura canal LoRa (LogDistance, 915 MHz), mobility constante y aplicaciones DV en todos los nodos.

## Métricas y trazas CSV
`metrics_collector` exporta al final de cada simulación:
- `mesh_dv_metrics_tx.csv`: timestamp(s), nodeId, seq, dst, ttl, hops, rssi(dBm), battery(mV), score, sf, energyJ, energyFrac, ok.
- `mesh_dv_metrics_rx.csv`: timestamp(s), nodeId, src, dst, seq, ttl, hops, rssi(dBm), battery(mV), score, sf, energyJ, energyFrac, forwarded.
- `mesh_dv_metrics_routes.csv`: timestamp(s), nodeId, destination, nextHop, hops, score, seq, action.
- `mesh_dv_metrics_delay.csv`: timestamp(s), src, dst, seq, hops, delay(s), bytes, sf, delivered (solo paquetes entregados en el GW).
- `mesh_dv_metrics_duty.csv`: nodeId, dutyUsed, txCount, backoffCount.
- `mesh_dv_metrics_energy.csv`: nodeId, energyInitialJ, energyConsumedJ, energyRemainingJ, energyFrac.
- `mesh_dv_metrics_overhead.csv`: trazas de control (si se registran).

Claves:
- TX/RX se deduplican por (nodeId/src, seq) para PDR en análisis.
- PDR por nodo = `rx_unique / tx_unique` donde `tx_unique` son pares (nodeId, seq) con `ok==1` y `rx_unique` son pares (src, seq) entregados en el GW.
- PDR global = `rx_unique_total / tx_unique_total`.

## Pipeline de análisis (`analysis/run_analysis.py`)
Uso:
```bash
python3 analysis/run_analysis.py --label "nEd=5_stop=120_ADR=on_duty=1%" --duty-limit 0.01
```
Qué hace:
- Lee los CSV generados en la raíz.
- Deduplica TX por (nodeId, seq) y RX en el GW por (src, seq) para PDR global y por nodo.
- Calcula retrasos (media/mediana/p50/p90/p99) a partir de `mesh_dv_metrics_delay.csv`.
- Hops promedio, distribución de SF, duty usado y energía restante.
- Guarda tablas en `analysis/results/<label>/` (e.g., `summary_metrics.csv`, `pdr_per_node.csv`, `sf_distribution.csv`, `energy_metrics.csv`).
- Genera figuras en `analysis/results/<label>/figs/`: `pdr_per_node.png`, `delay_cdf.png`, `hops_hist.png`, `sf_distribution.png`, `duty_used_per_node.png`, `energy_remaining_per_node.png`.

### Componentes clave y funciones destacadas
- `scratch/LoRaMESH-sim/mesh_dv_app.cc`
  - `StartApplication()`: registra callbacks, configura MAC/energía/routing y lanza beacons DV y generación de datos.
  - `L2Receive()`: punto de entrada de paquetes; decide si actualizar DV (broadcast), entregar al GW o reenviar unicast, registrando métricas RX.
  - `ForwardWithTtl()`: maneja forwarding con TTL, deduplicación y duty-cycle antes de enviar.
  - `BuildAndSendDv()`: prepara y envía beacons DV con las mejores rutas conocidas.
  - `SendDataPacket()`: genera tráfico de datos de ED→GW y encola para CSMA.
  - Hooks de métricas: `LogTxEvent`, `LogRxEvent` registran TX/RX con SF, energía y se actualiza `RecordE2eDelay` al llegar al GW.
- `scratch/LoRaMESH-sim/metrics_collector.{h,cc}`
  - `RecordTx/Rx`: guardan eventos con timestamp, SF, batería, energía y score.
  - `RecordRoute`: trazas DV (NEW/UPDATE/PURGE).
  - `RecordE2eDelay`: registra delay e2e al entregar en GW usando primer TX visto.
  - `RecordDuty`: dutyUsed, txCount, backoffCount por nodo.
  - `RecordEnergySnapshot`: energía inicial/restante/fracción por nodo.
  - `ExportToCSV`: escribe todos los CSV de métricas al final de la simulación.
- `src/loramesh/model/` (resumen por fichero):
  - `loramesh-metric-composite`: métrica de enlace (ToA+hops+RSSI+batería/energía).
  - `loramesh-adr-hopbyhop`: selección de SF según SNR por enlace.
  - `loramesh-mac-csma-cad`: CSMA/CAD con ventana de backoff y duty-cycle.
  - `loramesh-energy-model`: seguimiento de energía/voltaje por nodo (TX/RX/idle).
  - `loramesh-routing-dv`: tabla DV, actualización por beacons, selección de next-hop y anuncios.
- `analysis/run_analysis.py`
  - `compute_pdr()`: deduplica TX por (nodeId,seq) y RX en GW por (src,seq); calcula tx_unique, rx_unique y PDR por nodo y global.
  - `compute_delay_stats()`: media/mediana/p50/p90/p99 de delay(s).
  - `compute_hops()`: hops promedio y tabla por paquete entregado.
  - `compute_sf_distribution()`: distribución de SF.
  - Generadores de figuras: barras de PDR/SF/duty/energía y CDF de delay.

## Requisitos y compilación
- ns-3 3.45 (incluido en este árbol).
- Compilador C++20 y CMake (usando `./ns3` wrapper).
- Python 3 con `pandas` y `matplotlib` para el análisis.

Build:
```bash
./ns3 configure --enable-examples
./ns3 build
```

## Flujo típico de experimentos
1. Ejecutar simulación:
   ```bash
   ./ns3 run "scratch/LoRaMESH-sim/mesh_dv_baseline --nEd=5 --stopSec=120 --enableAdr=true --enableDuty=true --dutyLimit=0.01 --enablePcap=false"
   ```
2. Analizar resultados:
   ```bash
   python3 analysis/run_analysis.py --label "nEd5_120s_ADRon_duty1" --duty-limit 0.01
   ```
3. Revisar `analysis/results/<label>/` para CSV y PNG generados.

## Contribución y licencia
- Código base ns-3 bajo GPL-2.0-only (ver LICENSE).
- Aportaciones al simulador/analysis son bienvenidas mediante PRs o issues; mantén estilo C++/Python del proyecto y añade pruebas o comandos de reproducción.

Referencias adicionales: la documentación general de ns-3 está disponible en <https://www.nsnam.org>.
