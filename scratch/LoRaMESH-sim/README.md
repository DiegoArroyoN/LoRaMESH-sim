# LoRaMESH-sim

Simulador LoRa Mesh sobre ns-3 para campañas DV, CSMA/CAD, duty-cycle y análisis E2E.

## Estado operativo actual

- Default operativo:
  - `profile=pueyo2024_paper_like`
  - `wireFormat=pueyo7b`
- Soporte legacy conservado:
  - `profile=extended`
  - `wireFormat=v2`

- **RX Multi-SF por detección** (`SimpleGatewayLoraPhy`):
  - Barrido SF (`SfScanMin..SfScanMax`) con dwell `CadSymbols * Tsym(SF,BW)`.
  - Lock de recepción solo tras detección (no lock inmediato por `trueSF`).
  - **Single-demod**: un paquete a la vez.
- **CAD CSMA/LBT no-oracle** (`CsmaCadMac`):
  - Modelo default `CadDecisionModel=local_power`.
  - Busy por potencia local recibida (`GetRxPower`) + filtro SF/frecuencia.
  - Duración CAD default `CadDurationMode=sf_bw` (dependiente de SF/BW).
- **Beacons DV latest-only** (`MeshDvApp`):
  - Default `BeaconLatestOnly=true`.
  - En cola se conserva el beacon más reciente; beacons viejos pendientes se reemplazan.

## Qué contiene este repositorio

- Escenario principal: `mesh_dv_baseline.cc`
- Lógica de aplicación/routing local: `mesh_dv_app.cc/.h`
- NetDevice mesh: `mesh_lora_net_device.cc/.h`
- Métricas y exportación: `metrics_collector.cc/.h`
- Formatos on-air activos:
  - comparable Pueyo:
    - `data_wire_header_pueyo7b.*`
    - `beacon_wire_header_pueyo.*`
  - legado generico:
    - `data_wire_header_v2.*`
    - `beacon_wire_header_v2.*`
- Scripts de campañas: `run_*.py`, `run_*.sh`
- Documentación técnica: `FSD_LLD_Simulador_LoRaMESH.md`

## Prerrequisitos

Este repositorio está pensado para ejecutarse **dentro de un árbol ns-3**.

Requisitos mínimos:

- ns-3 compilable (recomendado ns-3-dev/ns-3.45+)
- Módulos disponibles en tu árbol:
  - `src/lorawan`
  - `src/loramesh`

Si falta `src/loramesh`, el escenario no compilará.

## Instalación recomendada

Desde la raíz de ns-3:

```bash
cd scratch
git clone https://github.com/DiegoArroyoN/LoRaMESH-sim.git LoRaMESH-sim
cd ..
./ns3 build
```

## Ejecución rápida

Desde la raíz de ns-3:

```bash
./ns3 run "scratch/LoRaMESH-sim/mesh_dv_baseline --profile=pueyo2024_paper_like --nEd=9 --nodePlacementMode=pueyo_grid --pueyoGridSpacingM=177 --trafficLoad=low --dataStartSec=300 --dataStopSec=3900 --stopSec=4500 --pdrEndWindowSec=600"
```

## Salidas

Cada corrida genera `mesh_dv_summary.json` y CSV de métricas en el directorio de ejecución.
Los scripts de campaña (`run_*.py`) guardan resultados en `validation_results/` (ignorado por git).

## Parámetros relevantes

- `--profile=pueyo2024_paper_like`
- `--wireFormat=pueyo7b`
- `--enableCsma=true|false`
- `--enableDuty=true|false`
- `--dutyLimit=0.01`
- `--dutyWindowSec=3600`
- `--trafficLoad=low|medium|high|saturation`
- `--beaconIntervalWarmSec`, `--beaconIntervalStableSec`
- `--routeTimeoutFactor`
- `--rngRun=<seed>`

Si necesitas reproducir campañas legacy o de ingenieria internas, `extended` y `v2` siguen soportados, pero ya no son el camino operativo recomendado.

Parámetros nuevos relevantes (atributos internos):

- `ns3::SimpleGatewayLoraPhy::EnableSfScanRx=true`
- `ns3::SimpleGatewayLoraPhy::SfScanCadSymbols=2`
- `ns3::loramesh::CsmaCadMac::CadDecisionModel=local_power`
- `ns3::loramesh::CsmaCadMac::CadDurationMode=sf_bw`
- `ns3::MeshDvApp::BeaconLatestOnly=true`

## Compartir con otra persona

Comparte este link:

- `https://github.com/DiegoArroyoN/LoRaMESH-sim`

Y pídele ejecutar exactamente los pasos de **Instalación recomendada** y **Ejecución rápida**.
