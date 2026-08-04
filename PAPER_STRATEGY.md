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
| **3. Replicación externa** | **¿reproduce lo de otro grupo?** | **NO HECHO — es el que falta** |

El nivel 3 es el que cierra la duda, y también el que preguntará el revisor.
Objetivo natural: **Pueyo-Centelles**, porque ya implementamos su métrica
fielmente (`toa_only`, ecuación 4) y su escenario (rejilla 177 m), y ellos
publicaron simulación **y hardware**.

Tres desenlaces, los tres útiles: coincide → V&V externa fuerte; no coincide y
sabemos por qué → hallazgo (ya tenemos uno); no coincide y no sabemos → hay que
arreglarlo **antes** de gastar 13 000 celdas.

### Hueco detectado

**δ nunca se ha probado sin duty cycle con el binario arreglado.** Todas las
campañas (E15, E17, E18, E19) usan `--allowDutyOverride=true`. El viejo
"+9.69% de FND sin duty" es del binario roto. Si δ funciona sin duty y muere con
duty, la conclusión deja de ser *"el término de energía no sirve"* y pasa a ser
**"la regulación de duty cycle es lo que lo anula"** — más citable, y alineado
con lo que el survey de ACM señala. ~200 celdas.

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
   de SF, con y sin lotería de batería.
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

## 5. Plan

1. **Replicación de Pueyo-Centelles** (~300 celdas). Cierra la pregunta del
   simulador con evidencia externa. **Bloquea todo lo demás.**
2. **δ sin duty cycle** (~200 celdas). Cierra el único hueco del claim.
3. **Gate G4** con S. Sobarzo y G. Saavedra sobre `DOE.md` v3 y este storyline.
   Es un cambio de tesis, no un ajuste.
4. **Campañas finales** (M1, M2, E6): ~13 000 celdas, 35–50 h.
5. **Reescritura**: conservar related work, system model y methodology; rehacer
   intro, results, discussion y conclusion.

Los pasos 1 y 2 son ~500 celdas y contestan las dos dudas de fondo antes de
comprometer nada.
