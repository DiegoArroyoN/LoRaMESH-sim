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

**Su figura 11a (rejilla 177 m, tráfico bajo) queda reproducida.** Seis puntos,
10 semillas cada uno, con margen 1 dB y SF ortogonales:

| N | 9 | 16 | 25 | 36 | 49 | 64 |
|---|---|---|---|---|---|---|
| Pueyo | 0.955 | 0.910 | 0.870 | 0.830 | 0.790 | 0.730 |
| nosotros | 0.9569 | 0.9305 | 0.9036 | 0.8524 | 0.7934 | 0.7395 |
| desviación | +0.2% | +2.3% | +3.9% | +2.7% | +0.4% | +1.3% |

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
- E22 (recortar la baliza a su precio): el PDR **baja**, no sube — con 2 rutas por
  emisión se ahorra aire pero la convergencia se degrada más de lo que compensa.
  Óptimo interior en 18 rutas, y ni ese se acerca a su curva.
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

1. **δ nunca se ha probado sin duty cycle con el binario arreglado.** Todas las
   campañas de δ (E15, E17, E18, E19, E25) usan `--allowDutyOverride=true`. El
   viejo "+9.69% de FND sin duty" es del binario roto. Si δ funciona sin duty y
   muere con duty, la conclusión deja de ser *"el término de energía no sirve"* y
   pasa a ser **"la regulación de duty cycle es lo que lo anula"** — más citable,
   y alineado con lo que el survey de ACM señala. ~200 celdas. **Es el hueco de
   más valor esperado que queda.**
2. **Las figuras 11c (rejilla 248 m) y 12 (aleatoria) no están digitalizadas.**
   Los valores de referencia que usamos ahí son aproximaciones nuestras, así que
   ninguna desviación de 248 m es reportable. Bloquea la mitad del capítulo de
   réplica, justo la mitad donde la facturación plana pega más fuerte.
3. **E22 corrió a margen 0.** El mecanismo del ToA es contabilidad de aire y no
   depende del margen, pero el óptimo interior de 18 rutas es un contraste
   pareado y hay que reverificarlo con margen 1 (mismo argumento que E24).

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

**Título de trabajo:** *Dónde va realmente la energía en LoRa mesh con duty
cycle: el plano de control domina a la métrica de ruteo en un orden de magnitud*

**Tesis:** bajo EU868 al 1%, el encaminamiento gobierna el **1.6%** del
presupuesto energético de un nodo. Ninguna métrica de ruteo puede mover la vida
útil desde ahí. Lo que sí la mueve —y a la vez la entrega— es el plan del plano
de control.

**Encuadre obligatorio (validación 2026-08-03):** el resultado se presenta como
**delimitación del dominio de validez** de una técnica que sí funciona en redes
de sensores clásicas, no como refutación general. La frontera es el duty cycle
regulatorio, que acota el TX a ≤36.4% del gasto y hace que domine la escucha.

### Cinco actos

1. **El presupuesto.** Reposo 45.7%, TX 33.2%, RX 19.2%, CAD 1.8%. Dentro del
   TX: balizas 65.4%, tráfico propio 29.7%, relevo 4.8% → el ruteo controla el
   **1.61%**. Techo estructural: en 72 escenarios el relevo nunca pasa del
   8.21% y el TX nunca del 36.4%, **porque el duty del 1% obliga a que la
   escucha domine**. La regulación pone el techo.
2. **El término de energía falla, y sabemos por qué.** δ **hace su trabajo**
   (reparte el relevo 22% mejor, 40/40 celdas, p<0.0001) y aun así acorta la
   vida. Mecanismo: esquivar a un vecino débil exige **subir el SF**; más SF
   reparte mejor pero cada transmisión dura el doble. El equilibrio cuesta más
   airtime del que ahorra. Verificado con δ ∈ [0, 0.85], 3 topologías, 2 rangos
   de SF, con y sin lotería de batería, **y con margen de SF de 0 y 1 dB
   (E25)** — este último cerraba la última duda, porque el margen sube el SF por
   su cuenta y podía haber quitado a δ su única palanca o habérsela dado. Ni lo
   uno ni lo otro: cada efecto de δ es del mismo signo con margen y **un poco
   mayor en la dirección mala** (t50 −0.86%→−1.22%, e_max +0.96%→+1.10%). El
   `e_max` sube en 27 de 30 semillas: δ hace que el nodo más castigado gaste
   *más*. Un solo contraste salió positivo (FND +0.56%) y es ruido, p=0.58.
3. **La palanca estaba en otro sitio.** Balizas de 60 s a 900 s: **+229% de PDR
   y +8.1% de FND**, las dos a la vez. La métrica: +4.7% de PDR a costa de
   −13.2% de FND. Cincuenta veces más efecto, y sin canje.
4. **Y esto invalida cómo el campo evalúa.** La ventaja de la compuesta pasa de
   +13.08% a +4.8% al sanear la cadencia, y su coste en vida útil se triplica.
   **Evaluar una métrica con una cadencia sin examinar mide sobre todo la
   cadencia.**
5. **Implicaciones de diseño.** Anuncio por evento en vez de por reloj.
   Receptor con ciclo de trabajo (el 45.7% intocado). Y el detalle que lo
   resume: **quitar el byte de SoC de la baliza ahorra 1.3–2.7% de la energía
   total — más de lo que el término de SoC podía mover en su techo teórico.**

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

1. **Digitalizar las figuras 11c y 12.** No es cómputo, es media hora de trabajo,
   y desbloquea la mitad del capítulo de réplica. **Camino crítico.**
2. **δ sin duty cycle** (~200 celdas). El hueco de más valor esperado que queda:
   puede convertir *"el término no sirve"* en *"la regulación lo anula"*.
3. **E22 con margen 1** (~300 celdas). Reverifica el óptimo interior del tamaño
   de baliza fuera del régimen frágil.
4. **Gate G4** con S. Sobarzo y G. Saavedra sobre `DOE.md` v3 y este storyline.
   Es un cambio de tesis, no un ajuste.
5. **Campañas finales** (M1, M2, E4, E5, E6): ~13 000 celdas, 35–50 h.
6. **Reescritura**: conservar related work, system model y methodology; rehacer
   intro, results, discussion y conclusion.

Los pasos 1–3 son ~500 celdas más media hora de digitalización, y cierran las
tres dudas que quedan antes de comprometer nada.

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
