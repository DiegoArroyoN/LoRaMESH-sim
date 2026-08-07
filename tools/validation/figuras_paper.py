#!/usr/bin/env python3
"""figuras_paper.py — el juego de figuras del paper, todas boxplot sobre semillas.

Por que boxplot y no linea de medias (DOE.md §10.4, regla añadida el 2026-08-06):
en varias campañas la dispersion entre semillas es del orden del efecto que se
reporta, y una linea de medias la esconde. Cada caja lleva su n en el pie.

Las figuras que emulan supuestos de OTRO grupo (ortogonalidad perfecta de SF,
facturacion de balizas a 12 B) se marcan como tales en el titulo: describen el
modelo de FLoRaMesh, no el comportamiento de DV-CL.

  python figuras_paper.py <dir_csv> <dir_salida>
"""
import sys, os, csv, collections
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

SRC = sys.argv[1] if len(sys.argv) > 1 else "."
DST = sys.argv[2] if len(sys.argv) > 2 else "figuras"
os.makedirs(DST, exist_ok=True)

C_REAL, C_ALT, C_ALT2, C_ALT3 = "#1f4e79", "#c1440e", "#2e7d32", "#6a1b9a"
C_REF = "#b00020"
plt.rcParams.update({"font.size": 9, "axes.grid": True, "grid.alpha": 0.3,
                     "axes.axisbelow": True, "figure.dpi": 150})


def load(name):
    p = os.path.join(SRC, name + ".csv")
    if not os.path.exists(p):
        print(f"  !! falta {p}")
        return []
    with open(p) as f:
        rows = [r for r in csv.DictReader(f) if r.get("rc") == "0"]
    return rows


def fnum(r, k):
    try:
        v = float(r[k])
        return None if (k in ("fnd_s", "t50_s") and v < 0) else v
    except Exception:
        return None


def boxes(ax, groups, labels, colors, width=0.6, positions=None):
    """groups: lista de listas de valores. Dibuja cajas con mediana y media."""
    pos = positions if positions is not None else range(len(groups))
    bp = ax.boxplot(groups, positions=list(pos), widths=width, patch_artist=True,
                    showmeans=True, meanline=False,
                    medianprops=dict(color="black", lw=1.4),
                    meanprops=dict(marker="D", ms=3.5, mfc="white", mec="black", mew=0.8),
                    flierprops=dict(marker="o", ms=2.5, mfc="#00000030", mec="none"),
                    whiskerprops=dict(lw=0.9), capprops=dict(lw=0.9))
    for patch, c in zip(bp["boxes"], colors):
        patch.set_facecolor(c)
        patch.set_alpha(0.55)
        patch.set_edgecolor(c)
        patch.set_linewidth(1.1)
    return bp


def pie(fig, caso, varia, emulacion=False):
    txt = f"[caso: {caso}]  varía: {varia}"
    if emulacion:
        txt += ("\n[EMULACIÓN de supuestos de FLoRaMesh — NO es el comportamiento "
                "de DV-CL]")
    fig.text(0.005, -0.035, txt, fontsize=6.6, va="top", ha="left",
             color="#b00020" if emulacion else "#333333")


def guardar(fig, nombre):
    fig.savefig(os.path.join(DST, nombre), bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  ok {nombre}")


# ---------------------------------------------------------------- F_replica
def f_replica():
    rows = load("e21b_replica_pueyo")
    if not rows:
        return
    NS = [9, 16, 25, 36, 49, 64]
    ARMS = [("0", "base", "margen 0 · Goursaud", C_REAL),
            ("0", "ortho", "margen 0 · SF ortogonales", C_ALT2),
            ("1", "base", "margen 1 dB · Goursaud", C_ALT3),
            ("1", "ortho", "margen 1 dB · SF ortogonales", C_ALT)]
    g = collections.defaultdict(list)
    for r in rows:
        if r["topo"] != "grid" or r["spacing"] != "177" or r["carga"] != "low":
            continue
        v = fnum(r, "pdr")
        if v is not None:
            g[(r["margen_db"], r["modelo"], int(r["nEd"]))].append(v)

    fig, ax = plt.subplots(figsize=(9.2, 4.6))
    w = 0.19
    for i, (mg, mod, lab, col) in enumerate(ARMS):
        data, pos = [], []
        for j, n in enumerate(NS):
            v = g.get((mg, mod, n), [])
            if v:
                data.append(v)
                pos.append(j + (i - 1.5) * w)
        if data:
            boxes(ax, data, None, [col] * len(data), width=w * 0.85, positions=pos)
        ax.plot([], [], color=col, lw=6, alpha=0.55, label=lab)

    ax.set_xticks(range(len(NS)))
    ax.set_xticklabels([str(n) for n in NS])
    ax.set_xlabel("nodos (N)")
    ax.set_ylabel("PDR")
    ax.set_ylim(0, 1.02)
    ax.set_title("Replicación de Pueyo-Centelles fig. 11a: hacen falta los dos cambios a la vez",
                 fontsize=10.5, pad=10)
    ax.text(0.99, 0.03, "comparación con su fig. 11a: SOLO de tendencia.\n"
                        "Sus figuras son boxplots y no publican valores.",
            transform=ax.transAxes, fontsize=6.8, ha="right", va="bottom", color="#555555",
            bbox=dict(fc="white", ec="#cccccc", lw=0.6, pad=3))
    ax.legend(fontsize=7.6, loc="lower left", framealpha=0.95)
    pie(fig, "rejilla 177 m, all-to-all, toa_only, SF7-8, ALOHA, sin duty, "
             "canal determinista, tráfico bajo, 10 semillas",
        "margen del selector de SF × modelo de interferencia")
    guardar(fig, "F_replica_177m.png")


# ------------------------------------------------------------ F_facturacion
def f_facturacion():
    rows = load("e26_facturacion_plana")
    if not rows:
        return
    NS = [9, 16, 25, 36, 49, 64]
    g = collections.defaultdict(list)
    tt = collections.defaultdict(list)
    for r in rows:
        v, t = fnum(r, "pdr"), fnum(r, "toa_baliza_ms")
        k = (r["factura"], r["spacing"], int(r["nEd"]))
        if v is not None:
            g[k].append(v)
        if t is not None:
            tt[k].append(t)

    fig, axes = plt.subplots(1, 3, figsize=(13.2, 4.4), constrained_layout=True)
    for ax, sp in zip(axes[:2], ["177", "248"]):
        for i, (fac, lab, col) in enumerate([("real", "baliza real (paga su aire)", C_REAL),
                                             ("plano", "facturada a 12 B (su modelo)", C_ALT)]):
            data, pos = [], []
            for j, n in enumerate(NS):
                v = g.get((fac, sp, n), [])
                if v:
                    data.append(v)
                    pos.append(j + (i - 0.5) * 0.34)
            if data:
                boxes(ax, data, None, [col] * len(data), width=0.30, positions=pos)
            ax.plot([], [], color=col, lw=6, alpha=0.55, label=lab)
        ax.set_xticks(range(len(NS)))
        ax.set_xticklabels([str(n) for n in NS])
        ax.set_xlabel("nodos (N)")
        ax.set_ylim(0, 1.02)
        ax.set_title(f"rejilla {sp} m", fontsize=10)
        ax.legend(fontsize=7.2, loc="lower left", framealpha=0.95)
    axes[0].set_ylabel("PDR")

    ax = axes[2]
    for i, (fac, lab, col) in enumerate([("real", "baliza real", C_REAL),
                                         ("plano", "facturada a 12 B", C_ALT)]):
        data, pos = [], []
        for j, n in enumerate(NS):
            v = tt.get((fac, "177", n), [])
            if v:
                data.append(v)
                pos.append(j + (i - 0.5) * 0.34)
        if data:
            boxes(ax, data, None, [col] * len(data), width=0.30, positions=pos)
        ax.plot([], [], color=col, lw=6, alpha=0.55, label=lab)
    ax.set_xticks(range(len(NS)))
    ax.set_xticklabels([str(n) for n in NS])
    ax.set_xlabel("nodos (N)")
    ax.set_ylabel("ToA media de baliza (ms)")
    ax.set_title("el mecanismo: su plano de control no escala", fontsize=10)
    ax.legend(fontsize=7.2, loc="upper left", framealpha=0.95)

    fig.suptitle("El supuesto que no se puede pagar: información de ruteo completa "
                 "al precio de un paquete mínimo", fontsize=11, y=1.0)
    pie(fig, "rejilla all-to-all, toa_only, SF7-8, ALOHA, sin duty, canal determinista, "
             "SF ortogonales, margen 1 dB, tráfico bajo, 10 semillas",
        "facturación del aire de la baliza × separación", emulacion=True)
    guardar(fig, "F_facturacion_plana.png")


# ---------------------------------------------------------------- F_balizas
def f_balizas():
    """N=25 y N=49 van en filas SEPARADAS. Mezclarlos en una caja no muestra
    dispersion entre semillas: superpone dos niveles distintos, infla el rango y
    esconde el efecto."""
    rows = load("e20_frontera_balizas")
    if not rows:
        return
    BS = [60, 150, 300, 600, 900, 1800, 3600, 7200, 14400]
    g = collections.defaultdict(lambda: collections.defaultdict(list))
    for r in rows:
        if r["cfg"] != "comp":
            continue
        try:
            b, n = int(r["baliza_s"]), int(r["nEd"])
        except Exception:
            continue
        for k in ("pdr", "fnd_s", "frac_baliza"):
            v = fnum(r, k)
            if v is not None:
                g[k][(n, b)].append(v)

    espec = [("pdr", "PDR", C_REAL, 1.0),
             ("fnd_s", "FND (ks)", C_ALT2, 1e-3),
             ("frac_baliza", "fracción del aire\nen balizas", C_ALT, 1.0)]
    fig, axes = plt.subplots(2, 3, figsize=(13.0, 7.4), constrained_layout=True)
    for fila, n in enumerate([25, 49]):
        for ax, (k, lab, col, esc) in zip(axes[fila], espec):
            data, pos = [], []
            for j, b in enumerate(BS):
                v = g[k].get((n, b), [])
                if v:
                    data.append([x * esc for x in v])
                    pos.append(j)
            if data:
                boxes(ax, data, None, [col] * len(data), width=0.62, positions=pos)
            ax.set_xticks(range(len(BS)))
            ax.set_xticklabels([str(b) for b in BS], rotation=45, ha="right", fontsize=7.5)
            ax.set_ylabel(lab, fontsize=8.5)
            if fila == 1:
                ax.set_xlabel("intervalo de baliza (s)")
            ax.axvline(4, color=C_REF, ls=":", lw=1.2)
        axes[fila][0].text(0.02, 0.96, f"N = {n}", transform=axes[fila][0].transAxes,
                           fontsize=10, fontweight="bold", va="top")
    for fila in (0, 1):
        axes[fila][0].text(4.15, 0.03, "900 s", fontsize=7, color=C_REF,
                           va="bottom", ha="left", transform=axes[fila][0].get_xaxis_transform())
    fig.suptitle("La palanca real: espaciar las balizas mejora entrega y vida útil "
                 "a la vez, sin canje", fontsize=11.5)
    pie(fig, "rejilla 178 m, all-to-all, métrica compuesta (δ=0), CSMA/CAD, duty 1%, "
             "300 ks, 20 semillas por caja",
        "intervalo de baliza (filas: tamaño de red)")
    guardar(fig, "F_frontera_balizas.png")


# ------------------------------------------------------------------ F_delta
def f_delta():
    """DIFERENCIAS PAREADAS, no niveles. Es lo que mide el test de signos: para
    cada semilla y cada N, cuanto cambia la metrica al activar delta frente a
    delta=0 en esa misma semilla. Un boxplot de niveles mezclaria la varianza
    entre semillas -- que es grande -- con el efecto pareado, que es pequeño, y
    daria cajas solapadas donde el contraste es sistematico."""
    rows = load("e25_delta_margen")
    if not rows:
        return
    idx = {}
    for r in rows:
        idx.setdefault((r["margen_db"], r["nEd"], r["seed"]), {})[r["cfg"]] = r

    CONTR = [("d25", "δ = 0.25", C_ALT2), ("d85", "δ = 0.85", C_ALT),
             ("toaref", "toa_only", "#777777")]
    espec = [("fnd_s", "Δ FND (ks)", 1e-3), ("t50_s", "Δ T50 (ks)", 1e-3),
             ("e_max", "Δ consumo del nodo\nmás castigado (mAh)", 1.0),
             ("pdr", "Δ PDR", 1.0)]

    fig, axes = plt.subplots(1, 4, figsize=(14.0, 4.6), constrained_layout=True)
    for ax, (k, lab, esc) in zip(axes, espec):
        data, pos, cols = [], [], []
        for i, mg in enumerate(["0", "1"]):
            for j, (cfg, clab, col) in enumerate(CONTR):
                d = []
                for key, cell in idx.items():
                    if key[0] != mg or cfg not in cell or "d00" not in cell:
                        continue
                    a, b = fnum(cell[cfg], k), fnum(cell["d00"], k)
                    if a is not None and b is not None:
                        d.append((a - b) * esc)
                if d:
                    data.append(d)
                    pos.append(i * 4 + j)
                    cols.append(col)
        if data:
            boxes(ax, data, None, cols, width=0.68, positions=pos)
        ax.axhline(0, color="black", lw=1.2, ls="-", zorder=0)
        ax.set_xticks([1, 5])
        ax.set_xticklabels(["margen 0 dB", "margen 1 dB"])
        ax.set_ylabel(lab, fontsize=8.5)
        ax.axvline(3, color="#999999", lw=0.8, ls="--")
    axes[2].text(0.5, 0.97, "↑ peor", transform=axes[2].transAxes, fontsize=8,
                 ha="center", va="top", color=C_REF, fontweight="bold")
    axes[0].text(0.5, 0.03, "↓ peor", transform=axes[0].transAxes, fontsize=8,
                 ha="center", va="bottom", color=C_REF, fontweight="bold")
    for cfg, clab, col in CONTR:
        axes[0].plot([], [], color=col, lw=6, alpha=0.55, label=f"{clab} − (δ=0)")
    axes[0].legend(fontsize=7.2, loc="upper left", framealpha=0.95)
    fig.suptitle("δ no rescata la vida útil, tampoco con margen: cada efecto es del "
                 "mismo signo y algo mayor en la dirección mala", fontsize=11.5)
    pie(fig, "rejilla 178 m all-to-all (máximo relevo), SF7-8, balizas 900 s, CSMA/CAD, "
             "duty 1%, N∈{25,49}, 300 ks, 15 semillas → 30 pares por caja",
        "peso δ × margen del selector de SF — DIFERENCIAS PAREADAS contra δ=0 "
        "en la misma semilla")
    guardar(fig, "F_delta_margen.png")


# ----------------------------------------------------------------- F_margen
def f_margen():
    rows = load("e23_margen_sf")
    if not rows:
        return
    MG = ["0", "1", "2", "3", "6"]
    SP = ["177", "200", "240", "248", "260"]
    g = collections.defaultdict(list)
    for r in rows:
        if r["rango"] != "sf78":
            continue
        v = fnum(r, "pdr")
        if v is not None:
            g[(r["margen_db"], r["spacing"])].append(v)

    fig, ax = plt.subplots(figsize=(9.6, 4.4))
    cols = [C_REAL, C_ALT2, C_ALT3, "#00838f", C_ALT]
    for i, mg in enumerate(MG):
        data, pos = [], []
        for j, sp in enumerate(SP):
            v = g.get((mg, sp), [])
            if v:
                data.append(v)
                pos.append(j + (i - 2) * 0.17)
        if data:
            boxes(ax, data, None, [cols[i]] * len(data), width=0.15, positions=pos)
        ax.plot([], [], color=cols[i], lw=6, alpha=0.55, label=f"margen {mg} dB")
    ax.set_xticks(range(len(SP)))
    ax.set_xticklabels([f"{s} m" for s in SP])
    ax.set_xlabel("separación de la rejilla")
    ax.set_ylabel("PDR")
    ax.set_ylim(-0.02, 1.0)
    ax.axvspan(1.6, 3.4, color=C_REF, alpha=0.08)
    ax.text(2.5, 0.93, "banda muerta con margen 0:\nel selector declara viable un enlace\n"
                       "con 0.04–0.40 dB de reserva",
            fontsize=7, ha="center", color=C_REF)
    ax.set_title("Sin reserva, el selector de SF elige enlaces que no sobreviven a nada",
                 fontsize=10.5, pad=10)
    ax.legend(fontsize=7.6, loc="upper left", framealpha=0.95)
    pie(fig, "rejilla all-to-all, toa_only, SF7-8, ALOHA, sin duty, canal determinista, "
             "N=25, tráfico bajo, 10 semillas",
        "margen del selector de SF × separación")
    guardar(fig, "F_margen_sf.png")


# ------------------------------------------------------------ F_tamano_baliza
def f_tamano():
    rows = load("e22_coste_baliza")
    if not rows:
        return
    PB = [7, 28, 56, 112, 251]
    RUTAS = {7: 2, 28: 9, 56: 18, 112: 37, 251: 83}
    NS = [9, 16, 25, 36, 49, 64]
    g = collections.defaultdict(list)
    for r in rows:
        v = fnum(r, "pdr")
        if v is not None:
            g[(int(r["payload_b"]), int(r["nEd"]))].append(v)

    fig, ax = plt.subplots(figsize=(9.6, 4.4))
    cols = [C_ALT, "#e07b39", C_ALT2, C_ALT3, C_REAL]
    for i, pb in enumerate(PB):
        data, pos = [], []
        for j, n in enumerate(NS):
            v = g.get((pb, n), [])
            if v:
                data.append(v)
                pos.append(j + (i - 2) * 0.17)
        if data:
            boxes(ax, data, None, [cols[i]] * len(data), width=0.15, positions=pos)
        lab = f"{RUTAS[pb]} rutas ({pb} B)"
        if pb == 7:
            lab += " ← su facturación"
        if pb == 251:
            lab += " ← nuestra base"
        ax.plot([], [], color=cols[i], lw=6, alpha=0.55, label=lab)
    ax.set_xticks(range(len(NS)))
    ax.set_xticklabels([str(n) for n in NS])
    ax.set_xlabel("nodos (N)")
    ax.set_ylabel("PDR")
    ax.set_title("Recortar la baliza no rescata la entrega: el óptimo es interior, "
                 "y la baliza mínima es la peor", fontsize=10.5, pad=10)
    ax.legend(fontsize=7.4, loc="upper right", framealpha=0.95)
    pie(fig, "rejilla 177 m all-to-all, toa_only, SF7-8, ALOHA, sin duty, canal "
             "determinista, SF ortogonales, margen 0, tráfico bajo, 10 semillas",
        "capacidad de la baliza (rutas por emisión)", emulacion=True)
    guardar(fig, "F_tamano_baliza.png")


for fn in (f_replica, f_facturacion, f_balizas, f_delta, f_margen, f_tamano):
    print(f"- {fn.__name__}")
    try:
        fn()
    except Exception as e:
        print(f"  !! {type(e).__name__}: {e}")
print(f"\nfiguras en {DST}/")


# =========================================================================
# Réplica de las ocho subfiguras de Pueyo-Centelles (fig. 11 y fig. 12).
#
# ESTRUCTURA DE SUS FIGURAS, leída de los pies del PDF (págs. 15-16):
#   fig. 11 = topología REJILLA        fig. 12 = topología ALEATORIA
#            (a) 177 m, tráfico bajo            (a) área equiv. 177 m, bajo
#            (b) 177 m, tráfico alto            (b) área equiv. 177 m, alto
#            (c) 248 m, tráfico bajo            (c) área equiv. 248 m, bajo
#            (d) 248 m, tráfico alto            (d) área equiv. 248 m, alto
#   filas = separación, columnas = carga. Y el eje y CAMBIA de escala entre
#   tráfico bajo (0..1) y alto (0..0.25); ellos lo avisan en el pie.
#
# LO ÚNICO QUE VARIAMOS: el modelo de interferencia. Margen fijo en 1 dB.
# NO se dibuja ninguna línea con "sus valores": sus figuras son boxplots y no
# publican cifras, así que la comparación es de TENDENCIA, visual.
# =========================================================================
def f_pueyo_paneles():
    rows = load("e21b_replica_pueyo")
    if not rows:
        return
    NS = [9, 16, 25, 36, 49, 64]
    MODS = [("base", "Goursaud (interferencia inter-SF)", C_REAL),
            ("ortho", "SF perfectamente ortogonales (su modelo)", C_ALT)]
    SUB = [("a", "177", "low"), ("b", "177", "high"),
           ("c", "248", "low"), ("d", "248", "high")]

    g = collections.defaultdict(list)
    for r in rows:
        if r["margen_db"] != "1":
            continue
        v = fnum(r, "pdr")
        if v is not None:
            g[(r["modelo"], r["topo"], r["spacing"], r["carga"], int(r["nEd"]))].append(v)

    for topo, fignum, tlabel in (("grid", "11", "rejilla"), ("random", "12", "aleatoria")):
        fig, axes = plt.subplots(2, 2, figsize=(11.4, 8.0), constrained_layout=True)
        nmin = 10 ** 9
        for ax, (letra, sp, carga) in zip(axes.ravel(), SUB):
            vmax = 0.0
            for i, (mod, lab, col) in enumerate(MODS):
                data, pos = [], []
                for j, n in enumerate(NS):
                    v = g.get((mod, topo, sp, carga, n), [])
                    if v:
                        data.append(v)
                        pos.append(j + (i - 0.5) * 0.34)
                        nmin = min(nmin, len(v))
                        vmax = max(vmax, max(v))
                if data:
                    boxes(ax, data, None, [col] * len(data), width=0.30, positions=pos)
                ax.plot([], [], color=col, lw=6, alpha=0.55, label=lab)
            ax.set_xticks(range(len(NS)))
            ax.set_xticklabels([str(n) for n in NS])
            ax.set_xlabel("número de nodos")
            ax.set_ylabel("PDR medio")
            # Ellos usan 0..1 en tráfico bajo y 0..0.25 en alto. En bajo coincide;
            # en alto NO se puede copiar su límite porque nuestro PDR lo supera y
            # recortaríamos datos reales -- y precisamente el que los superemos es
            # parte de lo que la figura muestra. Se ajusta a los datos y se avisa.
            if carga == "low":
                ax.set_ylim(0, 1.0)
                nota = ""
            else:
                ax.set_ylim(0, max(0.25, vmax * 1.08))
                nota = "  [su eje homólogo llega a 0.25]"
            area = "área equivalente a " if topo == "random" else ""
            ax.set_title(f"({letra}) {area}$d_x$ = {sp} m, tráfico "
                         f"{'bajo' if carga == 'low' else 'alto'}{nota}", fontsize=9.6)
            ax.legend(fontsize=7.2, loc="upper right", framealpha=0.95)
        fig.suptitle(f"Réplica de Pueyo-Centelles fig. {fignum} — topología {tlabel}. "
                     f"Nota: la escala del eje y cambia entre tráfico bajo y alto",
                     fontsize=11.5)
        ns = nmin if nmin < 10 ** 9 else 0
        pie(fig, f"topología {tlabel}, all-to-all, toa_only, SF7-8, ALOHA, sin duty, "
                 f"canal determinista, margen de SF 1 dB, {ns} semillas por caja",
            "modelo de interferencia × separación × carga de tráfico")
        fig.text(0.005, -0.075,
                 "Comparación con su figura homóloga: SOLO de tendencia y comportamiento. "
                 "Sus figuras son boxplots y no publican valores numéricos.",
                 fontsize=6.6, va="top", ha="left", color="#555555")
        guardar(fig, f"F_pueyo_fig{fignum}_{topo}.png")


f_pueyo_paneles()
