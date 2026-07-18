# LoRaMESH-sim

Simulador LoRa mesh sobre ns-3 para campa?as comparables con Pueyo-Centelles y para estudios controlados de MAC, routing, energ?a y contenci?n PHY.

## Qu? documento leer primero

Este repositorio ya no debe usarse con un solo documento para todo. La separaci?n correcta es:

- `README.md`
  - punto de entrada
  - mapa documental
  - comandos m?nimos de build y ejecuci?n
- `AI_OPERATOR_GUIDE.md`
  - gu?a operativa completa para otra IA o para trabajo remoto reproducible
  - campa?as, rutas, comandos, m?tricas, pitfalls, checklist
- `FSD_LLD_Simulador_LoRaMESH.md`
  - arquitectura y dise?o del simulador
  - componentes, perfiles, PHY, MAC, routing, wire formats, supuestos f?sicos

Si otra IA va a trabajar con el simulador, el orden correcto es:

1. leer `AI_OPERATOR_GUIDE.md`
2. leer `FSD_LLD_Simulador_LoRaMESH.md`
3. si hay contexto experimental activo, leer el handoff remoto vigente

Regla r?pida:

- estado de campa?as, comandos y operaci?n remota: `AI_OPERATOR_GUIDE.md`
- arquitectura, perfiles y dise?o interno: `FSD_LLD_Simulador_LoRaMESH.md`

## Secuencia recomendada de trabajo

Para evitar errores metodologicos o de operacion, la secuencia correcta es:

1. confirmar host, repo activo y commit
2. verificar si hay `tmux` o campa?as activas
3. leer la semantica del baseline en `AI_OPERATOR_GUIDE.md`
4. si hay duda de arquitectura o de un contador, ir al `FSD`
5. si se cambia codigo:
   - editar
   - compilar solo el target necesario
   - correr un smoke test corto
   - recien despues lanzar una campa?a

## Entorno operativo actual

- host remoto: `ns3-remote`
- repo activo: `/home/diego/sim/current-ns3`
- repo congelado real: `/home/diego/sim/LoRaMESH-sim-frozen-20260327`
- commit freeze: `bed544a`
- tag: `loramesh-sim-frozen-20260327`

Baseline principal actual:

- `profile = pueyo2024_paper_like`
- `topology = pueyo_grid`
- `trafficMode = pueyo_all_to_all`
- `100` paquetes por par origen-destino
- cargas:
  - `low = 100 s`
  - `medium = 10 s`
  - `high = 1 s`
  - `saturation = 0.1 s`
- baseline paper-like estricto:
  - `SF7-8`
  - `enable_csma = false`
  - `enable_duty = false`
  - `pueyoFloraLikeRx = true`
  - `EnableSfScanRx = false`

## Quick start

### Conexi?n remota

Desde este entorno Windows conviene desactivar multiplexing al usar SSH:

```powershell
ssh.exe -o ControlMaster=no -o ControlPath=none ns3-remote
```

### Build recomendado

Desde la ra?z de ns-3 en el remoto:

```bash
cd /home/diego/sim/current-ns3
cmake --build build -j 1 --target ns3-dev-mesh_dv_baseline-default
```

No se recomienda usar build global si no es necesario. Puede fallar por targets ajenos al escenario.

### Corrida r?pida

```bash
cd /home/diego/sim/current-ns3
./ns3 run "scratch/LoRaMESH-sim/mesh_dv_baseline --profile=pueyo2024_paper_like --nEd=9 --nodePlacementMode=pueyo_grid --pueyoGridSpacingM=177 --trafficLoad=low --dataStartSec=300 --dataStopSec=39400 --stopSec=40000 --pdrEndWindowSec=600"
```

### Verificaci?n r?pida de una campa?a

```bash
find /home/diego/sim/current-ns3/scratch/LoRaMESH-sim/validation_results/<campana> -name row.json | wc -l
```

## D?nde quedan los resultados

- corridas y campa?as activas:
  - `/home/diego/sim/current-ns3/scratch/LoRaMESH-sim/validation_results`
- archivo principal consolidado:
  - `/home/diego/sim/results_archive/pueyo2024_paper_like_campaigns_20260406`

## Qu? no hacer

- no usar `PeriodicFlush`
- no convertir el baseline en TDMA
- no introducir m?s `reception_paths`
- no asumir que el cuello principal es routing
- no interpretar `eventos por paquete generado` como si fueran porcentajes de paquetes

## Troubleshooting minimo

- si `ssh` falla de forma intermitente:
  - usar `ssh.exe -o ControlMaster=no -o ControlPath=none ns3-remote`
- si `scp` falla en este Windows:
  - usar `ssh.exe ... "cat archivo"` y redirigir localmente
- si una campa?a parece terminada pero hay duda:
  - contar `row.json`
- si el build global falla:
  - compilar solo `ns3-dev-mesh_dv_baseline-default`
- si aparece `generated_count < workload_target`:
  - no interpretarlo como p?rdida PHY; revisar primero el cierre temporal de generaci?n

## Diagn?stico t?cnico resumido

En el baseline actual, el problema dominante es PHY/canal compartido:

- `rx_no_more_demodulators`
  - receptor ocupado cuando llega otra transmisi?n
- `rx_post_lock_interference_fail`
  - interferencia despu?s de que una se?al ya hab?a hecho lock
- `pueyo_same_sf_overlap_events`
  - s?ntoma fuerte de solapamiento same-SF

En el baseline paper-like, un nodo solo puede recibir un paquete a la vez. Por eso la contenci?n temporal del canal domina mucho m?s que `drop_no_route`.

## Estado de documentaci?n

Este `README` es un mapa de entrada y no intenta duplicar el FSD.

Para trabajo serio con el simulador:

- operaci?n y campa?as:
  - `AI_OPERATOR_GUIDE.md`
- arquitectura y dise?o:
  - `FSD_LLD_Simulador_LoRaMESH.md`

