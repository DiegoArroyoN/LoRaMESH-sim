# Simulador NS3 Remoto Handoff

Fecha: 2026-03-31

## 1. Entorno remoto

- Host SSH: `ns3-remote`
- Repo activo: `/home/diego/sim/current-ns3`
- Repo real: `/home/diego/sim/LoRaMESH-sim-frozen-20260327`
- Commit: `bed544a`
- Tag: `loramesh-sim-frozen-20260327`

## 2. Estado del simulador

- Perfil baseline principal: `pueyo2024_paper_like`
- Topologia baseline: `pueyo_grid`
- Cargas:
  - `low = 1 paquete cada 100 s`
  - `medium = 1 paquete cada 10 s`
  - `high = 1 paquete cada 1 s`
  - `saturation = 1 paquete cada 0.1 s`
- Trafico baseline: `pueyo_all_to_all`
- Carga por par origen-destino: `100` paquetes
- PDR: cuenta solo paquetes de datos end-to-end. Los beacons no entran al PDR.

## 3. Defaults actuales importantes

- `policy1` quedo embebida como default operativo de la base Pueyo.
- Semantica de `policy1`:
  - `allowTemporalDesyncVariant = true`
  - `dataStartPhaseMaxSec = 100`
  - sin jitter por periodo
  - sin slots
- `paper_like` mantiene baseline estricto `SF7-8`.
- Existe override experimental para abrir SF en `paper_like`, pero solo si se pide explicitamente.
- `PeriodicFlush` fue eliminado de los runners activos de campanas porque corrompia `delivered/PDR`.

## 4. Fisica/modelo que se esta usando

- `pueyo2024_paper_like` usa:
  - `pueyoFloraLikeRx = true`
  - `EnableSfScanRx = false`
  - `interferenceModel = pueyo_fixed_capture`
- Consecuencias:
  - el receptor no se queda fijo en un SF; hace lock inmediato al SF real de la senal entrante si esta libre
  - un nodo solo puede recibir un paquete a la vez
  - `rx_no_more_demodulators` significa receptor ocupado
  - `rx_post_lock_interference_fail` significa que un paquete ya lockeado murio despues por interferencia
  - en `pueyo_fixed_capture`, la destruccion post-lock relevante es sobre todo `same-SF`
  - `cross-SF` no destruye al paquete lockeado en la logica `PUEYO_FIXED_CAPTURE`, pero tampoco habilita recepcion concurrente real

## 5. Lectura tecnica ya cerrada

- El cuello principal del baseline no es routing.
- Los problemas dominantes son:
  - `rx_no_more_demodulators`
  - `rx_post_lock_interference_fail`
  - `pueyo_same_sf_overlap_events`
- `drop_no_route` se mantiene bajo y `source_first_tx_count` es casi igual a `generated_count`.

## 6. Perfiles activos

- `pueyo2024`
- `pueyo2024_paper_like`
- `proposal_pueyo_like`
- `proposal_pueyo_like_observed`
- `extended` queda como legacy

Notas:
- `proposal_pueyo_like_observed` usa `sfLinkMode=observed_rxsf`.
- `pueyo2024_paper_like` es el baseline principal actual.

## 7. Campanas ya corridas

### 7.1 Low, 177 m, 5 seeds, baseline principal

- Escenario:
  - `pueyo2024_paper_like`
  - `policy1`
  - `pueyo_grid`
  - `177 x 177 m`
  - `low`
  - `SF7-8`
  - `N = 9,16,25,36,49,64`
  - `5` seeds por N
- Resultados locales:
  - `C:\Users\Diego\Documents\Playground\remote-results\analysis\pueyo_policy1_full_sweep_20260328_5seeds_report.md`
  - `C:\Users\Diego\Documents\Playground\remote-results\analysis\pueyo_policy1_full_sweep_20260328_5seeds_results_raw.csv`
- PDR medio:
  - `N9 = 66.61%`
  - `N16 = 54.03%`
  - `N25 = 41.62%`
  - `N36 = 33.62%`
  - `N49 = 27.40%`
  - `N64 = 21.01%`

### 7.2 High, 177 m, 5 seeds, baseline principal

- Escenario:
  - mismo baseline anterior
  - `high`
- Resultados locales:
  - `C:\Users\Diego\Documents\Playground\remote-results\analysis\pueyo_policy1_full_sweep_high_20260328_205631\report.md`
  - `C:\Users\Diego\Documents\Playground\remote-results\analysis\pueyo_policy1_full_sweep_high_20260328_205631\results_raw.csv`
- PDR medio:
  - `N9 = 9.72%`
  - `N16 = 4.93%`
  - `N25 = 2.21%`
  - `N36 = 1.50%`
  - `N49 = 0.97%`
  - `N64 = 0.61%`
- Interpretacion:
  - en `high` el PDR colapsa rapido
  - domina aun mas `rx_no_more_demodulators`
  - el delay promedio se mantiene bajo, asi que la red descarta mucho mas de lo que encola

### 7.3 Ablacion de rango SF

- Local:
  - `C:\Users\Diego\Documents\Playground\remote-results\analysis\sf_range_ablation_20260328_172005_report.md`
  - `C:\Users\Diego\Documents\Playground\remote-results\analysis\sf_range_ablation_20260328_172005_results_raw.csv`
- Resultado principal:
  - mantener `SF7-8` como baseline paper-like
  - `SF7-9` y `SF7-10` quedan como variantes de mitigacion
  - `SF7-12` empeora por exceso de airtime

## 8. Campana actualmente corriendo

- Nombre de sesion `tmux`: `pueyo_low_248_sweep`
- Outdir remoto:
  - `/home/diego/sim/current-ns3/scratch/LoRaMESH-sim/validation_results/pueyo_policy1_full_sweep_low_248m_20260331_20260331_090617`
- Escenario:
  - `pueyo2024_paper_like`
  - `policy1`
  - `pueyo_grid`
  - `248 x 248 m`
  - `low`
  - `SF7-8`
  - `N = 9,16,25,36,49,64`
  - `5` seeds por N

Comandos utiles:

```powershell
ssh -t ns3-remote "tmux attach -t pueyo_low_248_sweep"
```

```powershell
ssh ns3-remote "tail -f /home/diego/sim/current-ns3/scratch/LoRaMESH-sim/validation_results/pueyo_policy1_full_sweep_low_248m_20260331_20260331_090617/launcher.log"
```

## 9. Runners y archivos utiles

- Runner de campanas grid:
  - `C:\Users\Diego\Documents\Playground\remote-tools\run_pueyo_grid_campaign.py`
- Helper principal del baseline local:
  - `C:\Users\Diego\Documents\Playground\remote-tools\mesh_dv_baseline.cc`
- Handoff anterior del proyecto:
  - `/home/diego/sim/current-ns3/CODEX_PUEYO_HANDOFF_20260327.md`

## 10. Prompt corto para un hilo nuevo

Usa este contexto:

- Repo remoto activo: `/home/diego/sim/current-ns3`
- Commit freeze: `bed544a`
- Baseline principal: `pueyo2024_paper_like`
- Topologia baseline: `pueyo_grid`
- Trafico baseline: `pueyo_all_to_all`, `100` paquetes por par
- `policy1` ya esta por defecto
- `paper_like` baseline estricto es `SF7-8`
- Cuello principal: `rx_no_more_demodulators`, luego `rx_post_lock_interference_fail`
- No reactivar `PeriodicFlush`
- Revisar primero los reportes ya generados en `remote-results/analysis`

