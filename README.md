# LoRaMESH-sim

Implementación en NS-3 de un protocolo de enrutamiento mesh DV para LoRa con plano de control y de datos separados (SF/potencia diferenciados), CSMA/backoff para respetar duty-cycle y métricas exportadas a CSV.

## Versiones probadas / dependencias
- NS-3: `ns-3-dev` (git `ns-3.45-0-g4059af204`).
- Módulos: `core`, `network`, `mobility`, `lorawan`, `energy`.
- PHY: `SimpleGatewayLoraPhy` (multi-SF) en todos los nodos.
- Frecuencia: 915 MHz, canal `LogDistancePropagationLossModel` (ref=7.7 dB a 1 m, exp=2.7), `ConstantSpeedPropagationDelayModel`.

## Estructura del código
- `mesh_dv_app.*`: aplicación DV; balizas de control, reenvío de datos, CSMA/backoff, duty-cycle, modelo de batería y cálculo de score (ToA + hops + RSSI + batería).
- `mesh_lora_net_device.*`: NetDevice LoRa minimalista; selecciona SF/potencia según tag (control vs datos) y registra RX/TX.
- `mesh_metric_tag.*`: tag con campos de métrica (src/dst/ttl/hops/sf/rssi/batt/score) y payload de rutas Top-N en balizas.
- `metrics_collector.*`: recolector y exportador de TX/RX/rutas a CSV.
- `mesh_dv_baseline.cc`: escenario lineal parametrizable (EDs + GW/destino).

## Plano de control vs datos (implementado)
- Control (balizas DV, `dst=0xFFFF`): SF12, potencia 20 dBm, payload Top-N de mejores rutas; calcula ToA y respeta duty-cycle.
- Datos (unicast): SF9 por defecto (tag), potencia 10 dBm.
- Receptor multi-SF: `SimpleGatewayLoraPhy` permite recibir control y datos con SF distintos en todos los nodos.
- CSMA + backoff + duty-cycle: antes de cada TX se verifica ventana libre y presupuesto de duty-cycle.

## Cómo usar
1. Copia los archivos en `scratch/mesh_dv/` dentro de tu árbol de NS-3 (`ns-3-dev`).
2. Desde la raíz de NS-3:
   ```bash
   ./waf --run "scratch/mesh_dv/mesh_dv_baseline --nEd=3 --spacing=30 --stopSec=150"
   ```
   - `nEd`: cantidad de nodos mesh (el nodo `nEd` es el destino/GW).
   - `spacing`: separación lineal entre nodos (m); usa valores en metros (p.ej. 85000 = 85 km) para forzar multihop.
   - `stopSec`: duración de la simulación.
3. Al finalizar se exportan CSV en el directorio de trabajo:
   - `mesh_dv_metrics_tx.csv`
   - `mesh_dv_metrics_rx.csv`
   - `mesh_dv_metrics_routes.csv`

## Campos de métricas (CSV)
- TX: `timestamp(s)`, `nodeId`, `seq`, `dst`, `ttl`, `hops`, `sf`, `rssi(dBm)`, `battery(mV)`, `score`, `ok`.
- RX: `timestamp(s)`, `nodeId` (receptor), `src`, `dst`, `ttl`, `hops`, `sf`, `rssi(dBm)`, `battery(mV)`, `score`, `forwarded`.
- Rutas: `timestamp(s)`, `nodeId`, `destination`, `nextHop`, `hops`, `score`, `seq`, `action` (NEW/UPDATE/EXPIRE).

## Escenario baseline actual
- Topología lineal: nodos 0..`nEd-1` en el eje X, separados por `spacing`; nodo `nEd` es el destino (GW) a `gwHeight` metros.
- PHY multi-SF en todos los nodos, misma frecuencia 915 MHz y paths de recepción múltiple.
- Canal: pérdida log-distance (exp=2.7) para incentivar multihop; la potencia no se toca en datos (10 dBm) salvo balizas (20 dBm).
- Aplicación: balizas cada 60 s (`m_period`), TTL=10 saltos, score inicial 100; CSMA/backoff habilitado.

## Ejemplos de corrida
- Corrida usada para pruebas de largo alcance y multihop:
  ```bash
  ./ns3 run "scratch/mesh_dv/mesh_dv_baseline --spacing=85000 --stopSec=150"
  ```
  Resultado observado (spacing 85 km, SF12 control 20 dBm / SF9 datos 10 dBm): PDR al destino ≈66.7% (6/9 paquetes), saltos hasta 3, RSSI en destino -131 a -121 dBm. CSVs generados: `mesh_dv_metrics_tx_85km_control20_data10_sf12_sf9.csv`, `mesh_dv_metrics_rx_85km_control20_data10_sf12_sf9.csv`, `mesh_dv_metrics_routes_85km_control20_data10_sf12_sf9.csv` (no se versionan por tamaño).

## Notas y buenas prácticas
- Mantén habilitado CSMA/backoff para no saturar el canal y respetar duty-cycle.
- El score pondera ToA, hops, RSSI y batería (se conserva la métrica original propuesta).
- `.gitignore` excluye artefactos de simulación (CSV/PNG/HTML) para no subir resultados pesados.
- Si modificas SF o potencias, hazlo consistente con el tag en `MeshDvApp` y el comportamiento en `MeshLoraNetDevice`.
