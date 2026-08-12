# Convocatoria — Gate G4 (borrador para enviar)

**Para:** Sergio Sobarzo, Gabriel Saavedra
**Asunto sugerido:** G4 — decisión sobre el cambio de tesis del paper (JNCA)

---

Hola Sergio, Gabriel:

Os convoco al **gate G4** del paper. Es una reunión de decisión, no de avance:
desde la última revisión la tesis del manuscrito **ha cambiado**, y necesito
vuestra aprobación antes de comprometer el cómputo final.

**Duración:** 90 minutos.
**Lectura previa (imprescindible):** `G4.md` — 15 minutos. Si solo tenéis cinco,
leed la §1.

## Por qué este gate no es como los anteriores

Los tres gates previos certificaban que el simulador funcionaba. Este pide
aprobar **qué dice el paper**. Han cambiado dos cosas que no estaban previstas:

1. **La réplica de Pueyo-Centelles pasó de verificación a aportación.**
   Reproducimos sus ocho subfiguras y, por el camino, pusimos precio a las tres
   decisiones de modelado que sostienen su resultado.
2. **El término de energía δ pasó de "no sirve" a "la regulación lo anula".**
   Sin duty cycle mejora la vida útil del primer nodo un **+11.95%** (40 de 40
   semillas); bajo EU868 al 1% el mismo término, en el mismo escenario y con las
   mismas semillas, no hace **nada** (−0.05%, no significativo).

## Qué se decide

| # | decisión |
|---|---|
| **D1** | ¿se adopta la tesis nueva? *Es la única que, si no sale, invalida el resto* |
| **D2** | ¿el duty cycle es régimen principal o **factor** de primer nivel? |
| **D3** | ¿FND o T50 como métrica de vida útil? δ las intercambia |
| **D4** | E4c (US915) revive — con la recomendación **invertida** respecto al DoE v3 |
| **D5** | ¿se publica la retractación del umbral de SF como lección metodológica? |
| **D6** | **cómo se define la métrica**: α deja de ser grado de libertad |
| — | autorizar **~13 800 celdas** de cómputo |

**D6 es bloqueante.** Si la definición de la métrica cambia después de lanzar la
matriz factorial, ese cómputo se pierde entero.

## Lo que NO se discute

Está cerrado con evidencia y listado en `G4.md` §2 para que no se re-abra:
réplica, atribución de la brecha, presupuesto energético, cadencia de balizas,
capacidad óptima de baliza y pesos. Son ~9 000 celdas medidas.

## Lo que conviene que sepáis antes de entrar

El documento incluye **cinco riesgos declarados y varias retractaciones nuestras**,
entre ellas una crítica que le hacíamos al trabajo ancla y que resultó ser un
defecto propio. Preferimos que salgan en la reunión y no en revisión.

## Disponibilidad

¿Me decís vuestros huecos para la semana del **__ de agosto**? Propongo:

- Opción A: ___
- Opción B: ___

Un saludo,
Diego
