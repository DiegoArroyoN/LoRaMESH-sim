# Datos de las campañas reportables (margen 1 dB)

**Por qué están aquí.** Estos CSV vivían solo en `ns3-remote:~/ns3-runs/`. El
servidor se cayó el 2026-07-17 y volvió a caerse el 2026-08-10 mientras se
lanzaba E29. Son el respaldo de lo único que el paper puede citar: las campañas
medidas con `sfLinkMarginDb = 1.0`.

Por decisión de Diego del 2026-08-07 **no se analiza nada medido a margen 0**, así
que las campañas anteriores no se respaldan aquí — están en el servidor y su
valor fue encontrar los once defectos, no medir.

| fichero | qué establece | celdas |
|---|---|---:|
| `e21b_replica_pueyo.csv` | réplica de las ocho subfiguras de Pueyo-Centelles, 20 semillas por caja | 3 840 |
| `e28.csv` | capacidad óptima de baliza: escala con N, entre el 50% y el 77% de las rutas conocidas | 600 |
| `e20_static_m1.csv` | frontera de cadencia: 60→900 s da +229.4% de PDR; la curva satura sin punto de giro | 720 |
| `e7_static_m1.csv` + `e7_none_m1.csv` | no hay umbral de SF; el colapso a PDR 0.0001 era defecto nuestro | 640 |
| `e16_frontera_malla_m1.csv` | pesos: la ventaja viene de tener β>0, no de ajustarlo | 480 |
| `e14_energia_m1.csv` | presupuesto energético: reposo 46.46%, el ruteo gobierna el 2.21% | 320 |
| `e27.csv` | **δ sin duty: el signo cambia. Es la tesis del paper** | 240 |
| `e26_facturacion_plana.csv` | precio de la facturación de 12 B de FLoRaMesh | 240 |
| `e25_delta_margen.csv` | δ = 0 también con margen | 240 |

**Los scripts que las produjeron** están en `tools/validation/`, y cada celda
archiva su `mesh_dv_effective_config.csv` en el servidor. La columna `sfmargin`
(añadida el 2026-08-07, 36 columnas) permite verificar a qué margen corrió
cualquier celda.
