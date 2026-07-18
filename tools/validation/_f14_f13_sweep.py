import csv, glob, os
roots = ["_a2a_full_runs_6b","_a2a_ext_runs_6b","_conv_full_runs_6b","_msink_full_runs_6b","soc_abl_runs_6b","wsweep2_runs_6b"]
base = "/home/diego/sim"
dc1_max = (0.0, ""); dcoff_max = (0.0, ""); closure_max = (0.0, ""); frac_max = (0.0, "")
n_dc1 = n_dcoff = n_energy = viol_duty = viol_close = 0
for root in roots:
    for duty in glob.glob(os.path.join(base, root, "*", "mesh_dv_metrics_duty.csv")):
        d = os.path.dirname(duty); name = os.path.join(root, os.path.basename(d))
        is_dc1 = "dc1" in os.path.basename(d)
        is_dcoff = "dcoff" in os.path.basename(d)
        try:
            rows = list(csv.DictReader(open(duty)))
        except Exception:
            continue
        for r in rows:
            u = float(r["dutyUsed"])
            if is_dc1:
                n_dc1 += 1
                if u > dc1_max[0]: dc1_max = (u, name + " node" + r["nodeId"])
                if u > 0.0101: viol_duty += 1
            elif is_dcoff:
                n_dcoff += 1
                if u > dcoff_max[0]: dcoff_max = (u, name + " node" + r["nodeId"])
        en = os.path.join(d, "mesh_dv_metrics_energy.csv")
        if os.path.exists(en):
            for r in csv.DictReader(open(en)):
                n_energy += 1
                ini = float(r["energyInitialJ"]); con = float(r["energyConsumedJ"]); rem = float(r["energyRemainingJ"])
                err = abs(ini - con - rem)
                if err > closure_max[0]: closure_max = (err, name + " node" + r["nodeId"])
                if err > 0.001: viol_close += 1
                if ini > 0:
                    fe = abs(float(r["energyFrac"]) - rem/ini)
                    if fe > frac_max[0]: frac_max = (fe, name + " node" + r["nodeId"])
print("F1.4a duty (corpus 6B):")
print("  nodos-run dc1: %d | violaciones (>1.01%%): %d" % (n_dc1, viol_duty))
print("  max dutyUsed dc1  = %.5f  @ %s" % dc1_max)
print("  max dutyUsed dcoff= %.5f  @ %s  (n=%d, sin limite: solo referencia)" % (dcoff_max[0], dcoff_max[1], n_dcoff))
print("F1.3a cierre de energia:")
print("  nodos-run: %d | violaciones (>1 mJ): %d" % (n_energy, viol_close))
print("  max |ini-con-rem| = %.6f J @ %s" % closure_max)
print("  max |energyFrac - rem/ini| = %.2e @ %s" % frac_max)
print("VEREDICTO: %s" % ("PASS ambos" if (viol_duty == 0 and viol_close == 0) else "VIOLACIONES - revisar"))
