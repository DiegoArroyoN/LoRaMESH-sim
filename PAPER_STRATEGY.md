# PAPER_STRATEGY.md — Reencuadre del manuscrito tras la depuración del simulador

**2026-08-03.** Documento de decisión, escrito a petición de Diego tras nueve
campañas nuevas (E13–E20, ~4 400 celdas) que invalidan el borrador `paper_v2`.
Acompaña a `DOE.md` v3 (commit `e64cc7e16`). Pendiente de validación
bibliográfica fresca — la búsqueda de NotebookLM tiene más de seis meses.

---

## 1. Auditoría de `paper_v2`: qué muere y qué sobrevive

### Muerto

**El tercio energético de C2 ("división de labores").** `results.tex` afirma que
δ *"eleva la batería del nodo más agotado entre +0.7 y +2.1 puntos... sin costo
de PDR (|ΔPDR| ≤ 0.2 pt)"*, con p<10⁻²⁹. Medimos lo contrario: **la baja 1.27% y
cuesta 4.85% de PDR**. Y la letra pequeña del propio borrador dice *"en
all-to-all **sin DC**"* — o sea que se midió fuera del régimen que el DoE v2
declaraba principal.

**La sinergia MAC×métrica de C1.** La ablación 2×2 es válida como método; la
lectura no. La interacción medida es **negativa** (−8.21 pp): el MAC rinde
*menos* con la métrica compuesta, no más.

**Todos los números de `results.tex`.** No por falsos uno a uno, sino porque
salieron de un binario con nueve defectos (entre ellos `sfMax=8`, que truncaba
el alcance a 350 m y costaba 19–24% de PDR, y la histéresis condicionada al modo
de métrica). No hay forma de saber cuáles sobrevivirían sin re-medir.

### Vivo

- **~50 KB de manuscrito**: `related_work.tex`, `system_model.tex` y buena parte
  de `methodology.tex`. Describen protocolo, modelo y método, no resultados.
- **Una frase de C3 que resultó profética**: *"el techo de PDR bajo el 1% es una
  propiedad de la configuración y no de la regulación"*. Se afirmó sin
  mecanismo. Ahora lo hay: la configuración es la cadencia de balizas, que a
  60 s consume el **74.9% del aire**, y arreglarla **triplica el PDR**.
- **La columna "Ablación causal"** de la tabla comparativa: ✗ en las ocho obras
  previas, ✓ solo en DV-CL. Ese es el activo real, y es lo que produjo el
  resultado negativo. No somos los que se equivocaron: somos **los únicos que
  comprobaron**.

---

## 2. Confianza en el simulador: los tres niveles

| nivel | qué es | estado |
|---|---|---|
| 1. Consistencia interna | ¿hace lo que dice? | **hecho**: 147 parámetros auditados, 8 defectos silenciosos arreglados, rastro de config por celda, 2 360 celdas verificadas, determinismo bit-idéntico |
| 2. Validación del modelo físico | ¿los modelos son fieles? | **hecho**: ToA vs Semtech AN1200.13, sensibilidades SX1276, pérdidas FLoRa, ALOHA vs S=G·e^(−2G) |
| **3. Replicación externa** | **¿reproduce lo de otro grupo?** | **HECHO 2026-08-06** — ver abajo |

### 3. Replicación externa: el resultado (E21b, E22, E26)

Objetivo: **Pueyo-Centelles 2024**, porque implementamos su métrica fielmente
(`toa_only`, ecuación 4) y su escenario (rejilla 177 m), y ellos publicaron
simulación **y hardware**.

**Sus ocho subfiguras (11a-d rejilla, 12a-d aleatoria) están replicadas** con
margen 1 dB y los dos modelos de interferencia, 20 semillas por caja (E21b+E21c,
3 840 celdas, 96 cajas completas).

> ⚠️ **RETRACTO 2026-08-07 — la comparación es de TENDENCIA, no de números.**
> Sus figuras son **boxplots y no publican valores numéricos**. Durante días usé
> `{0.955, 0.910, 0.870, 0.830, 0.790, 0.730}` como "sus valores publicados" de
> la fig. 11a y sobre ellos monté un titular de *"réplica reproducida, +0.2% a
> +3.9%"*. **Esos seis números no tenían fuente verificable**: aparecían solo en
> tres ficheros que había escrito yo. Tomé una nota propia anterior como si fuera
> una fuente. Al renderizar el PDF y leer su serie ToA, los valores reales están
> ~0.012–0.016 por encima, y el sesgo iba en la dirección que nos favorecía.
>
> **Ninguna figura nuestra lleva ya una línea con sus valores**, y no se reporta
> ninguna desviación numérica contra sus figuras. La regla que sale de esto:
> *una nota propia anterior no es una fuente*.

**Lo que sí se puede afirmar, y es visual:** con margen 1 dB + SF ortogonales
reproducimos su tendencia y su nivel en tres de los cuatro paneles de tráfico
bajo, **incluidos los dos de topología aleatoria**, que son los más difíciles de
acertar por casualidad porque su dispersión entre semillas es grande y también la
reproducimos. Las dos discrepancias: en **tráfico alto entregamos más que ellos**
(su eje llega a 0.25 y nuestro PDR alcanza 0.49), y en la **rejilla de 248 m en
tráfico bajo** divergemos de forma creciente con N — que es donde su facturación
de control les regala más.

**Y llegar ahí costó dos cosas, que hay que reportar separadas porque son de
naturaleza distinta:**

| factor | de quién | qué vale |
|---|---|---|
| margen de SF de 1 dB | **defecto nuestro**: el selector declaraba viable un enlace a 0.1 dB de la sensibilidad y el receptor le exigía además sobrevivir a la matriz de aislamiento 6×6. Dos componentes que no se hablaban | ×1.67 → ×2.93 (N=9 → 64) |
| ortogonalidad perfecta de SF | **supuesto suyo**, y su propio texto lo contradice: la pág. 2 dice "cuasi-ortogonales" y cita el trabajo sobre ortogonalidad imperfecta, pero su simulador implementa la perfecta | ×1.35 → ×1.61 |

No se suman, se potencian: la interacción crece de ×1.20 a ×1.85 con N. Tiene
sentido físico — sin margen no hay enlaces que la ortogonalidad pueda salvar.

**El tercer supuesto, y el que no se puede pagar (E22, E26).** FLoRaMesh hace
`routingPacket->setByteLength(routingPacketMaxSize=12B)`: en OMNeT++ las entradas
de ruta viven en una estructura que nunca se serializa, así que su modelo entrega
información de ruteo **completa** al precio de un paquete **mínimo**. Su plano de
control no escala con la red por construcción. Medido:

- ToA media de baliza, nuestra: 99.9 → 399.5 ms entre N=9 y N=64. La suya: **65.9 ms plana**.
- E28 (recortar la baliza a su precio, 600 celdas a margen 1): el PDR **baja**,
  no sube — con 2 rutas por emisión se ahorra aire pero la convergencia se
  degrada más de lo que compensa, y cuesta **×3.15** de entrega a 64 nodos. El
  óptimo es interior y **escala con N**: 9 rutas a N=16, 18 a N=25-36, 37 a
  N=49-64, siempre entre el 50% y el 77% de las rutas conocidas.
- E26 (su modelo exacto: 12 B de aire con las rutas completas, vía tag): sube el
  PDR ×1.13 a 177 m y ×1.34 a 248 m en N=64, y la fracción de aire en balizas cae
  del 66% al 23%.

**Conclusión:** su resultado exige información completa a precio mínimo, y esa
combinación no existe en el espacio de diseño. No es que entreguemos menos que
ellos: es que ese punto no es realizable.

**Lo que no cuadra, y se dice como tal.** Con su facturación adoptada, nuestro
simulador **supera** sus cifras publicadas por un margen creciente (+0.6% en N=9,
+14.9% en N=64). Su curva publicada queda **acotada entre nuestro modelo fiel y
nuestra emulación de sus supuestos**. No tenemos explicación cerrada; los
candidatos son el jitter `uniform(0,120)` del intervalo de anuncio, que no
podemos emular, y que nuestro brazo emulado lleva el margen de 1 dB que ellos no
necesitan.

### Huecos que siguen abiertos

1. ~~δ nunca se ha probado sin duty cycle~~ **CERRADO 2026-08-08 con E27**, y
   con el desenlace bueno: δ **sí funciona** sin duty (FND +13.41%, 30/30
   semillas) y **cambia de signo** con duty (−1.01%). La conclusión pasa de *"el
   término de energía no sirve"* a **"la regulación de duty cycle es lo que lo
   anula"**. Ver el Acto 2. Registré la predicción contraria antes de mirar y era
   falsa: yo esperaba que el signo no cambiara.
2. **Las figuras 11c (rejilla 248 m) y 12 (aleatoria) no están digitalizadas.**
   Los valores de referencia que usamos ahí son aproximaciones nuestras, así que
   ninguna desviación de 248 m es reportable. Bloquea la mitad del capítulo de
   réplica, justo la mitad donde la facturación plana pega más fuerte.
3. ~~E22 corrió a margen 0~~ **CERRADO 2026-08-08 con E28** (600 celdas, los dos
   margenes en la misma campaña y el mismo binario). El óptimo interior
   sobrevive, se desplaza hacia más rutas y aparece una regla que a margen 0 no
   se veía: **escala con N y se queda entre el 50% y el 77% de las rutas
   conocidas** (9 rutas a N=16, 18 a N=25-36, 37 a N=49-64). Anunciar todas nunca
   es lo mejor. Y el precio de la facturación de FLoRaMesh pasa de ×1.4 a
   **×3.15** a 64 nodos.

4. **NUEVO — el resto de campañas de margen 0.** Decisión de Diego del
   2026-08-07: los resultados medidos a `sfLinkMarginDb = 0` no se analizan.
   Afecta a E1–E20 y E22 enteras. Se salvan δ=0 (E25 lo confirmó a margen 1, y
   E27 lo ha reemplazado), la cadencia de balizas (E24 dio +228.7% frente al
   +229.7% de E20) y la réplica. **Se caen** el presupuesto energético de E14
   —que abre el paper—, la frontera α/β de E16 —de la que salía la elección de
   pesos—, la curva de balizas más allá de 1800 s y el umbral de SF de E7.
   Relanzamiento en cola: **E14** (320 celdas, corriendo) → **E16** (480) →
   **E20 extendida** (400) → **E7** (640).

---

## 3. Posicionamiento — **validado 2026-08-03 con búsqueda fresca**

### 3.0 Resultado de la validación bibliográfica

Diego señaló que la búsqueda de NotebookLM tenía más de seis meses. Se
re-verificó cada afirmación del posicionamiento contra literatura 2025–2026.

**Se sostiene (cuatro de cinco):**

| afirmación | verificación |
|---|---|
| nadie ha aislado el término energético de una métrica de ruteo LoRa | ninguna búsqueda devuelve una ablación de ese tipo |
| nadie ha cuantificado el overhead de control como fracción de airtime | reconocido como problema (survey ACM, crítica de Meshtastic) pero **nunca medido** |
| nadie ha barrido la cadencia de balizas contra PDR/vida útil | el trabajo más cercano (Sensors 25(5):1602, 2025) **modela** la energía de balizas pero no barre la cadencia ni reporta su fracción |
| el competidor de abril-2026 no hace simulación a nivel de evento | confirmado: marco de instantáneas sobre grafo expandido por SF |

**Corroboración inesperada y valiosa:** la escucha continua **no es un
artefacto de nuestro modelo**. Meshtastic —estándar industrial de facto— tiene
la misma propiedad, y su limitación energética reconocida es exactamente *"el
consumo significativo por recepción casi continua de cada nodo"*. Detalle
adicional: Meshtastic usa preámbulo de 16 símbolos (igual que nuestra
configuración) precisamente para que el receptor pueda dormir entre ventanas.

**Trabajo reciente revisado y descartado como competencia:** LIMA (arXiv
2512.00161, dic-2025) aumenta LoRaWAN con routers mesh y reclama 12.6× menos
energía **de end-device contra LoRaWAN sin malla** — no usa término energético
en la métrica, **no modela duty cycle** y **no reporta vida útil ni FND**.

### 3.1 Lo que hay que CORREGIR del análisis inicial

**El resultado negativo NO puede afirmarse como general.** La literatura de
redes de sensores clásicas muestra de forma consistente que el término de
energía residual **sí** alarga la vida (LEACH y derivados, ACO, etc.). Afirmar
"el ruteo energy-aware no funciona" sería falso y el revisor lo tumbaría de
inmediato.

**El claim correcto es una condición de frontera, y es más interesante:**

> En redes de sensores clásicas el término de energía residual funciona porque
> la transmisión domina el gasto y no hay duty cycle regulatorio. En LoRa bajo
> EU868 al 1%, la regulación acota el TX a ≤36.4% de la energía y la escucha
> domina, de modo que el mismo mecanismo **no puede** funcionar. Identificamos
> el régimen en el que la técnica deja de aplicar, y por qué.

Eso convierte un resultado negativo en una **delimitación del dominio de
validez** de una técnica establecida — mucho más defendible y más citable.

### 3.1 bis — Validación bibliográfica del 2026-08-11 (tesis nueva)

Los dos hallazgos que nacieron después de la validación de agosto —el régimen
regulatorio (E27) y la normalización (E29/E30)— no se habían contrastado nunca
contra la literatura. Se hizo el 2026-08-11 y el resultado es desigual.

**SE SOSTIENE — nadie compara los dos regímenes regulatorios con el mismo
protocolo.** El campo está partido y nadie cruza la frontera:

- Udugampola et al. 2025 (arXiv 2510.03714) hacen ruteo energy-aware en LoRa mesh
  subterránea y declaran explícitamente *"the full 100% duty cycle can be
  utilized"*: eligen el entorno sin restricción porque no hay riesgo de
  interferencia. Su mecanismo funciona — reparto de batería equilibrado, más vida
  útil. **Es nuestra tesis vista desde el otro lado**, y confirma que el régimen
  sin duty es un despliegue real y no un brazo sintético. Pero no prueban con
  duty.
- El competidor de 2026 (Electronics 15(9):1872) sí tiene métrica compuesta con
  aire, batería y duty, muy parecida a la nuestra, pero trata el duty como
  **restricción dura de factibilidad**, no como factor. Y confirma su límite
  declarado: es un *"snapshot routing framework"* **sin simulación a nivel de
  evento**. Optimizan un grafo; no miden una red.

**NO SE SOSTIENE — la normalización no es un hallazgo nuestro.** Ver la
corrección completa en `G4.md` §D6 bis. Está documentado en el survey de RPL
(arXiv 1902.01888) desde 2019, tanto el término de energía demasiado pequeño para
tener efecto como el problema de acotar los pesos a [0,1]. Se reporta como
*"trampa conocida en RPL que reaparece sin señalar en LoRa mesh"*, con lo nuestro
siendo la cuantificación y el canje.

**Y el acoplamiento por cuantización tampoco es nuestro.** Llegué a decir que no
aparecía en ninguna búsqueda; sí aparece, y es viejo: es el problema de las
*narrow metrics* de IS-IS. La métrica de 6 bits limitaba a la vez el alcance
(coste de camino máximo 1023) y la granularidad (*"with only 64 possible values
per link, distinguishing between different bandwidth capacities became
difficult"*), y se resolvió con las *wide metrics* TLV. Nuestro campo de 1 byte
con SF7 y SF8 cayendo en la misma unidad es eso mismo.

**Conclusión de la comprobación: el paper tiene DOS hallazgos metodológicos, no
tres.** El tercero se reescribe como sección de discusión: *dos trampas clásicas
del ruteo —el escalado de términos compuestos y las métricas estrechas— reaparecen
sin señalar en LoRa mesh, y aquí está lo que cuestan medidas*. Eso es honesto,
útil y no se cae en revisión.

**Nota de método para mí mismo:** he reclamado novedad dos veces hoy sin
comprobarlo antes, y las dos han fallado. La comprobación bibliográfica va
**antes** de encuadrar algo como hallazgo, no después.

---

### 3.1 ter — Comprobación con la bibliografía cargada (NotebookLM, 2026-08-11)

Consultado el corpus completo de `Papers_Magister`. Resultado por hallazgo:

**HALLAZGO 1 — la cadencia de balizas domina. SE SOSTIENE, y se refina.**

- *"Ningún artículo del corpus realiza una campaña experimental o simulación
  barriendo de forma sistemática el intervalo o cadencia de los anuncios de
  enrutamiento"*. Solé et al. corren a cadencia fija (300 s; 60 s para
  Pueyo-Centelles). Leenders et al. 2023 sí barren la cadencia del **tráfico de
  datos**, pero dejan `Troute` fija en 6 h sin variarla.
- **La sobrecarga de control SÍ está reportada, pero en BYTES.** Solé et al.
  definen `ControlOverhead = (RoutingPacketSize + HeaderDataMessages) /
  DataMessagesPayload` y miden *"the total number of bytes"*: **19%** en un salto,
  100% en el último nodo de una cadena.
- Wong et al. 2024 dan la única cifra en aire, y es **analítica y de MAC, no de
  ruteo**: *"At 1% duty cycle, TX is only allowed 1/100 slots, meaning 10%
  bandwidth used just for synchronisation."*

> **La refinación que esto permite, y que es mejor que el claim original:** en
> LoRa, bytes y airtime **no son equivalentes** — el tiempo en aire escala con
> 2^SF, y nuestras balizas se emiten con SF probabilístico. Medir la sobrecarga
> de control en bytes la **infravalora**. Solé mide 19% en bytes; nosotros
> medimos **60–75% en aire** sobre el mismo tipo de plano de control. Esa brecha
> *es* el resultado, y la distinción bytes/airtime es la aportación conceptual.

**HALLAZGO 2 — el régimen regulatorio como frontera. CONFIRMADO NOVEDOSO**, y
ahora con el mapa exacto del corpus:

| trabajo | qué hace con el duty |
|---|---|
| Uduwaka et al. 2026 (voz sobre Meshtastic) | **el único** que contrasta EU 1% contra US sin restricción — pero solo mide el tiempo de transmisión de un mensaje de voz de 3 s, **no toca el ruteo** |
| Cotrim & Margi 2024 | lo modelan fijo al 1%, sin contraste |
| Chen et al. 2025 | fuerzan el 1%, sin régimen alternativo |
| Udugampola et al. 2025 | lo **ignoran** (100%), sin comparación cruzada |
| Arroyo Navarrete 2026 (nuestra propia propuesta previa) | lo adoptaba fijo al 1%, sin contraste |

**Nadie compara los dos regímenes para el ruteo.** Ni siquiera nosotros lo
hacíamos antes de E27.

---

### 3.2 Panorama competitivo

**Competidor directo:** *Multi-Criteria Optimization Mechanisms for LoRa Network
Topologies* (Electronics 15(9):1872, 28-abr-2026) propone una métrica de salto
compuesta y adimensional con airtime, uso de duty y penalizaciones de calidad.
**Pero es un marco de instantáneas** que resuelve caminos mínimos en un grafo
expandido por SF, *"sin simulación a nivel de eventos"*: sin MAC, sin
colisiones, sin energía en el tiempo, sin FND. Proponen el término y no
comprueban si entrega.

**Udugampola et al. (2025)** reclama 75% de ahorro energético **contra
flooding** — no dice nada sobre el aporte marginal del término energético.

**Patrón:** el campo lleva años añadiendo términos de energía a métricas de
ruteo LoRa y **nadie ha aislado si el término entrega**.

**Ganchos del survey de ACM CSUR (2024):** nombra el **overhead de control** como
desafío abierto y el **duty cycle** como la decisión más impactante para
eficiencia energética. Nuestros dos resultados principales responden a ambos.

---

## 4. Storyline propuesto

> **REESCRITO EL 2026-08-08 TRAS E27.** La tesis anterior era *"el término de
> energía no sirve"*. E27 (240 celdas, duty como factor pareado, margen 1 dB)
> demuestra que **sí sirve, y que lo que lo anula es la regulación**. El cambio
> es de fondo, no de matiz: pasa de ser un resultado negativo sobre nuestra
> propia métrica a ser un resultado positivo sobre el dominio de validez de una
> familia de técnicas.

**Título de trabajo:** *La regulación como frontera del ruteo consciente de
energía: el duty cycle EU868 anula un término que funciona sin él*

**Tesis:** el término energy-aware **funciona**: sin restricción de duty cycle
mejora el FND un **+13.4%**, con unanimidad en las 30 semillas. Bajo EU868 al 1%
el mismo término, en el mismo escenario y con las mismas semillas, **cambia de
signo** y pasa a ser levemente dañino. La regulación no atenúa el beneficio: lo
invierte.

**Y un segundo resultado que matiza al primero, dentro del propio régimen sin
duty:** δ sube el FND (+13.4%) mientras baja el T50 (−6.7%). No es una
contradicción sino la firma del balanceo de carga, confirmada por la dispersión
del relevo entre nodos (`rel_cv` **−31.1%**). Al igualar el reparto, el nodo más
castigado dura más y desaparecen los nodos poco cargados que antes sobrevivían
mucho tiempo. **δ desplaza vida útil del primer nodo al conjunto.** Cuál de las
dos métricas importa deja de ser un detalle y pasa a ser una decisión de diseño
que el paper explicita.

**Encuadre obligatorio (validación 2026-08-03, reforzado por E27):** el resultado
se presenta como **delimitación del dominio de validez** de una técnica que sí
funciona en redes de sensores clásicas, no como refutación general. Ahora esa
delimitación no es una interpretación nuestra: es una medida con el régimen
regulatorio como factor experimental, y la frontera está donde el duty cycle
convierte el aire en el recurso escaso.

### Cinco actos

1. **El presupuesto.** ✅ **REMEDIDO A MARGEN 1 dB** (E14, 320 celdas, 2026-08-09).
   La estructura sobrevive casi intacta:

   | | reposo | TX | RX | CAD |
   |---|---|---|---|---|
   | margen 0 (retirado) | 45.7% | 33.2% | 19.2% | 1.8% |
   | **margen 1 (vigente)** | **46.46%** | **33.71%** | **18.01%** | **1.83%** |

   **El ruteo gobierna el 2.21% de la energía total** (antes 1.61%), con un techo
   de 5.43% sobre las 320 celdas. El TX nunca pasa del **37.40%**: el duty del 1%
   obliga a que la escucha domine, y esa es la frontera estructural.

   **Y aparece un matiz que a margen 0 no se veía, que hay que reportar porque
   condiciona el titular:** el reparto del aire depende fuertemente del rango de
   SF.

   | rango | balizas | tráfico propio | relevo |
   |---|---|---|---|
   | SF7-12 | **75–80%** | 18–23% | **1.7–2.1%** |
   | SF7-8 | 42–44% | 46–47% | **10.3–10.8%** |

   Con el rango completo las balizas se emiten en SF repartidos hasta SF12 y su
   aire se dispara, dejando al relevo en el 2% del canal. Forzando la malla a
   SF7-8 las balizas se abaratan y el relevo sube al 10%. **"El ruteo gobierna una
   fracción pequeña" es cierto en los dos regímenes en energía total, pero por
   razones distintas**, y el paper debe declarar en cuál mide.
2. **El término de energía funciona, y la regulación lo anula.** Acto reescrito
   el 2026-08-08 con E27 (240 celdas, margen 1 dB, 15 semillas, el duty como
   factor **pareado**: mismas semillas, mismo escenario de máximo relevo, con y
   sin regulación).

   | contraste sobre el FND | sin duty | con duty 1% |
   |---|---|---|
   | δ = 0.25 frente a δ = 0 | **+10.26%** (30/30, p<0.001) | −0.86% (0/30, p<0.001) |
   | δ = 0.85 frente a δ = 0 | **+13.41%** (30/30, p<0.001) | −1.01% (0/30, p<0.001) |

   **El signo cambia**, y era el listón registrado antes de mirar: para sostener
   *"la regulación es lo que anula δ"* no bastaba con que el efecto se atenuara.
   Cambia, y con unanimidad en las 30 semillas de cada brazo.

   **El mecanismo, ahora completo.** δ iguala el reparto de relevo entre nodos
   (`rel_cv` **−31.05%** sin duty, −24.64% con él): eso lo hace en los dos
   regímenes. Lo que cambia es el precio. Igualar exige **subir el SF**, y cada
   transmisión en SF más alto dura el doble; bajo duty al 1% el aire es el
   recurso escaso y ese coste se cobra entero, así que el balanceo sale a
   pérdidas. Sin la restricción, el mismo balanceo se cobra en un recurso que
   sobra y el beneficio queda al descubierto. **La tensión no la crea el PHY: la
   crea la regulación.**

   **Y una salvedad que enriquece el resultado en vez de debilitarlo:** dentro
   del régimen sin duty, δ sube el FND (+13.41%) y **baja el T50 (−6.70%)**. Es
   la firma del balanceo de carga: al igualar el reparto, el nodo más castigado
   dura más y a la vez desaparecen los nodos poco cargados que antes sobrevivían
   mucho tiempo, de modo que el grueso muere junto. **δ desplaza vida útil del
   primer nodo al conjunto**, y cuál de las dos importa pasa a ser una decisión
   de diseño explícita en vez de un detalle.

   > ⚠️ **CONFUSOR DETECTADO 2026-08-11, y decisión de Diego: se reportan las dos
   > campañas con la explicación.** Los brazos de E27 movían **tres** cosas a la
   > vez, porque los pesos se usaban como un símplex que suma 1:
   >
   > | brazo | α | β | δ | H = (β/α)·C |
   > |---|---|---|---|---|
   > | d00 | 0.85 | 0.15 | 0 | 1493 ms |
   > | d25 | 0.60 | 0.15 | 0.25 | 2116 ms |
   > | **d85** | **0.00** | 0.15 | 0.85 | **∞ — sin término de ToA** |
   >
   > **Qué sobrevive.** El confusor es idéntico en los dos regímenes de duty, así
   > que la **interacción —el cambio de signo, que es la tesis— se mantiene**. Lo
   > que no está limpio es atribuirle el efecto a δ en solitario.
   >
   > **Y la dirección del sesgo juega a favor.** E29 mide que más H cuesta FND
   > (−11% al pasar de 129 ms a 1493 ms), así que el brazo d85 —con H infinito—
   > partía con desventaja. **El efecto real de δ sin duty es probablemente mayor
   > que el +13.41% medido.**
   >
   > **E31** (480 celdas, en cola) lo rehace con α=1, β'=0.01524 y paso=0.005
   > fijos en los cinco brazos, moviendo solo δ'. Es lo que E27 creía estar
   > haciendo. Equivalencia de escala: δ=0.25 era 1.59× el coste de un enlace →
   > δ'=0.05 es 2.18×; δ=0.85 era 5.41× → δ'=0.15 es 6.55×.
   >
   > **Las dos entran al manuscrito**, por la misma razón que la retractación del
   > umbral de SF: el paper sostiene que evaluar sin auditar el instrumento mide
   > sobre todo el instrumento, y aplicárnoslo a nosotros mismos es lo que da
   > autoridad al argumento.

   **Métricas que NO se pueden usar aquí, y hay que decirlo:** `e_max` y
   `soc_min` están saturadas — 240 celdas dan 6 valores distintos de `e_max`,
   todos en 225.000x mAh (el tope de batería), y `soc_min` tiene un único valor,
   0.000000. A 300 ks todos los nodos mueren y las dos métricas dejan de
   discriminar. El mismo problema apareció en E25. Los contrastes válidos son
   FND, T50, PDR y `rel_cv`.
3. **La palanca estaba en otro sitio.** Balizas de 60 s a 900 s: **+229% de PDR
   y +8.1% de FND**, las dos a la vez. Es el único de los actos con cifra de
   margen 0 que **ya está confirmado a margen 1**: E24 midió +228.7% frente al
   +229.7% de E20, prácticamente idéntico. Lo que falta es la curva más allá de
   1800 s, que E24 no cubrió.

   El contraste de la métrica sí se movió al corregir el margen: de +4.8% a
   **+3.62%** de PDR. Aun así el orden de magnitud entre las dos palancas se
   mantiene: la decisión de cadencia vale unas sesenta veces más que la de
   encaminamiento, y mejora las dos métricas a la vez mientras el ruteo tiene que
   elegir entre ellas — cosa que E27 acaba de precisar: incluso cuando δ
   funciona, elige entre FND y T50.
4. **Y esto invalida cómo el campo evalúa.** La ventaja de la compuesta pasa de
   +13.08% a +4.8% al sanear la cadencia, y su coste en vida útil se triplica.
   **Evaluar una métrica con una cadencia sin examinar mide sobre todo la
   cadencia.** El reemplazo a margen 1 sale de E24 (+3.62%).

   ✅ **E16 remedida a margen 1** (480 celdas, 2026-08-09) y la conclusión de los
   pesos **se confirma**: la ventaja viene de **tener** el término de saltos, no
   de ajustarlo.

   | | α/β de 0.95/0.05 a 0.30/0.70 | β=0 (toa_only) → β>0 |
   |---|---|---|
   | PDR (perf) | spread ~1.4%, **ninguno significativo** | **+8.76%**, 40/40, p<0.0001 |
   | PDR (life) | spread ~3.0% | **+14.66%**, 40/40, p<0.0001 |

   **α=0.85, β=0.15 es el punto de operación**, y ahora sobre datos válidos: gana
   los tres contrastes de vida útil frente a las cuatro alternativas (p<0.0001) y
   solo pierde en rendimiento por un 1.3–1.4% que no es significativo (p≈0.08).
   Coste declarado: −2.64% de FND frente a `toa_only`.

   Eso blinda la pregunta obvia del revisor —*"¿cómo calibraron α y β?"*— cuya
   respuesta pasa a ser **"no hizo falta calibrarlos, y aquí está el barrido de
   480 celdas que lo demuestra"**.

   **Y E27 le añade un segundo filo, más fuerte:** evaluar un término
   energy-aware **sin declarar el régimen regulatorio** mide sobre todo el
   régimen. El mismo δ, el mismo escenario y las mismas semillas dan +13.41% o
   −1.01% de FND según haya duty o no. Cualquier trabajo que reporte uno de los
   dos números sin decir cuál está midiendo, no es comparable.
5. **Implicaciones de diseño.** Anuncio por evento en vez de por reloj.
   Receptor con ciclo de trabajo (el 45.7% intocado, cifra pendiente de remedir).
   **δ se activa o no según el régimen regulatorio**, que es la implicación nueva
   y la más accionable: en despliegues fuera de EU868 —o bajo LBT, o con
   ciclo de trabajo relajado— el término paga; bajo EU868 al 1%, no.

   Y la **capacidad de baliza como parámetro de diseño con regla** (E28, 600
   celdas, margen 1): el óptimo no es un valor fijo sino que **escala con N y se
   queda entre el 50% y el 77% de las rutas conocidas** — 9 rutas a N=16, 18 a
   N=25-36, 37 a N=49-64. Anunciar *todas* las rutas nunca es lo mejor y a N=64
   cuesta un 4%. Recortarla a los 12 B que factura FLoRaMesh cuesta **×3.15** de
   entrega a 64 nodos.

6. **Auditoría de los supuestos de modelado del estado del arte.** Acto nuevo,
   incorporado el 2026-08-06 y hoy el más sólido del manuscrito: reproducimos la
   curva publicada del trabajo ancla y ponemos precio a las tres decisiones de
   modelado que la sostienen — ortogonalidad perfecta de SF (×1.35→×1.61),
   facturación plana del plano de control (×1.13→×1.34), y coherencia
   selector/PHY (×1.67→×2.93, este último defecto **nuestro**, no suyo). Que la
   combinación que ellos reportan **no sea físicamente realizable** es el
   hallazgo, y sale de una medida, no de una lectura del código.

**Sección aparte — crítica constructiva al paper ancla:** la ecuación (4) de
Pueyo-Centelles (`2^(SF−7)`) hace que subir un SF **duplique** el coste. En
canal determinista `toa_only` usa SF7 en el 99.4–100% de las transmisiones: es
**un protocolo de SF fijo disfrazado de métrica adaptativa**. A 177 m da igual
porque SF7 alcanza; a 247 m se hunde a PDR 0.0001. El sombreado lo enmascara
(250× → 2×), lo que explica por qué nadie lo había visto.

> ⚠️ **RETRACTADO 2026-08-09 con E7 a margen 1 dB (640 celdas, los dos canales).**
> El hundimiento a PDR 0.0001 **era nuestro, no suyo**. A margen 0 el enlace de
> 247 m quedaba 0.11 dB por encima de la sensibilidad de SF7, el selector lo
> declaraba viable y `toa_only` se quedaba clavado ahí: 99.7% de SF7 y colapso.
> Con margen 1 dB el selector **sube correctamente a SF8** —0.0% de SF7 a 247 m,
> SF medio 8.02— y el PDR pasa de 0.0001 a **0.0151**, unas 150 veces más.
>
> | canal | 177 m | 247 m | caída |
> |---|---|---|---|
> | determinista (su condición) | 0.0287 | 0.0151 | ×0.53 |
> | sombreado σ=3.57 dB | 0.0489 | 0.0276 | ×0.56 |
>
> Queda una degradación suave al alejar los nodos, **igual en los dos canales**,
> así que ni siquiera se sostiene que el sombreado enmascare un umbral: no hay
> umbral que enmascarar. La ecuación (4) sí sesga hacia SF bajos —`toa_only` usa
> 99.7% de SF7 a 177 m frente al 23.2% de la compuesta— pero con un selector
> coherente eso no produce ningún colapso.
>
> **Esta crítica sale del paper.** Acusar al trabajo ancla de un fallo que era
> nuestro habría sido el peor error posible del manuscrito, y solo se detectó
> porque Diego exigió no analizar nada medido a margen 0.

### Por qué no es un paper débil

Un revisor ve: un resultado positivo de ingeniería (+229% PDR por configuración),
un presupuesto cuantitativo inédito, un mecanismo explicado, una advertencia
metodológica que afecta a trabajos previos, guía de diseño con números, y un
artefacto liberado con 147 parámetros auditados. Y responde dos desafíos que un
survey mayor declara abiertos.

### El riesgo y su mitigación

Riesgo: que se lea como *"demostraron que su propia métrica no funciona"*.
**Mitigación por encuadre**: DV-CL deja de ser el producto y pasa a ser **el
instrumento**. El paper no propone una métrica; usa una métrica completamente
instrumentada para medir dónde están las palancas.

El paper que había decía *"añadimos un término y ganamos un 13%"*. El que hay
dice *"medimos que ese 13% era la cadencia, que el término de energía no puede
funcionar y por qué, y dónde está la palanca real"*. El segundo es más difícil
de escribir y mucho más difícil de refutar.

---

## 5. Plan — actualizado 2026-08-06

~~1. Replicación de Pueyo-Centelles~~ **HECHA**: E21b (2 880 celdas), E22 (300),
E23 (500), E24 (320), E25 (240), E26 (240). El nivel 3 de V&V está cerrado a
177 m y la réplica pasó de ser verificación a ser **aportación**.

~~2. δ sin duty cycle~~ **HECHA**: E27, 240 celdas. Cambió la tesis del paper.
~~3. E22 con margen 1~~ **HECHA**: E28, 600 celdas. El óptimo pasó a ser una regla.

1. **Relanzar lo que corrió a margen 0**, por orden de daño al manuscrito:
   **E14** (presupuesto energético, 320 celdas, *corriendo*) → **E16** (frontera
   α/β, 480) → **E20 extendida** (cadencias que E24 no cubrió, 400) → **E7**
   (umbral de SF, 640). Sin E14 no hay Acto 1 y sin E16 no hay forma de justificar
   los pesos que se reportan.
2. **Gate G4** con S. Sobarzo y G. Saavedra. Es un cambio de tesis, no un ajuste,
   y ahora hay dos: la réplica pasó de verificación a aportación, y δ pasó de
   fracaso a resultado condicionado por la regulación.
3. **Campañas finales** (M1, M2, E4, E5, E6): ~13 000 celdas, 35–50 h.
4. **Reescritura**: conservar related work, system model y methodology; rehacer
   intro, results, discussion y conclusion.

**Lo que NO está en el camino crítico y conviene decir por qué.** Digitalizar las
figuras 11c y 12 de Pueyo estaba como paso 1 el 2026-08-06. Ya no: al descubrir
que sus figuras son boxplots sin valores publicados, la comparación pasó a ser de
tendencia y **no se reporta ninguna desviación numérica contra ellas**. Digitalizar
no desbloquea nada porque no vamos a publicar esos números.

---

## 6. Nota sobre la disciplina de medida

Once defectos silenciosos encontrados y arreglados hasta el 2026-08-06. El patrón
se repite lo bastante como para escribirlo: **un parámetro que se acepta por
línea de órdenes y no cambia nada, o una campaña que devuelve su CSV completo sin
haber medido**. Ninguno daba error. Los que más costaron:

- El perfil pisaba ocho parámetros después de leer la línea de órdenes.
- `GetBeaconRouteCapacity` tenía un return temprano que hacía a
  `--dvBeaconMaxRoutes` código muerto (K=0,1,4,16 → corridas bit-idénticas).
- `sfLinkMarginDb=0` deprimía el PDR ×2.3 en nuestro propio punto de operación.
- `ProcessTxQueue` hacía `RemoveAllPacketTags()` y reponía solo el metric tag:
  la primera pasada de E26 corrió entera con el tag de rutas muerto, dando
  hops=0.0000 en 120 celdas con un PDR de 0.21 que superaba la guarda.

**Lección aplicada a todas las campañas nuevas:** cada una lleva una precondición
que verifica que el factor MUERDE antes de gastar una celda, y el invariante de
esa precondición no puede ser la métrica que la campaña mide. En E26 el invariante
correcto eran los saltos, no el PDR — el PDR cambia a propósito entre brazos.
