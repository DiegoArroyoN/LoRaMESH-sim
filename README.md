# LoRaMESH-sim (wire v2)

Simulador LoRa Mesh sobre ns-3 para campañas DV, CSMA/CAD, duty-cycle y análisis E2E.

## Qué contiene este repositorio

- Escenario principal: `mesh_dv_baseline.cc`
- Lógica de aplicación/routing local: `mesh_dv_app.cc/.h`
- NetDevice mesh: `mesh_lora_net_device.cc/.h`
- Métricas y exportación: `metrics_collector.cc/.h`
- Formato on-air v2:
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
./ns3 run "scratch/LoRaMESH-sim/mesh_dv_baseline --wireFormat=v2 --nEd=25 --nodePlacementMode=random --areaWidth=1000 --areaHeight=1000 --trafficLoad=medium --enableCsma=true --enableDuty=true --dutyLimit=0.01 --dataStartSec=300 --dataStopSec=3900 --stopSec=4500 --pdrEndWindowSec=600"
```

## Salidas

Cada corrida genera `mesh_dv_summary.json` y CSV de métricas en el directorio de ejecución.
Los scripts de campaña (`run_*.py`) guardan resultados en `validation_results/` (ignorado por git).

## Parámetros relevantes

- `--wireFormat=v2`
- `--enableCsma=true|false`
- `--enableDuty=true|false`
- `--dutyLimit=0.01`
- `--dutyWindowSec=3600`
- `--trafficLoad=low|medium|high|saturation`
- `--beaconIntervalWarmSec`, `--beaconIntervalStableSec`
- `--routeTimeoutFactor`
- `--rngRun=<seed>`
