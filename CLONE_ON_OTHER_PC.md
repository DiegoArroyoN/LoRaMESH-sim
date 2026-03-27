# Clone on another PC

Preferred clean method from the generated bundle:

```bash
mkdir -p ~/sim
cd ~/sim
git clone /path/to/LoRaMESH-sim-frozen-20260327.bundle LoRaMESH-sim-frozen-20260327
cd LoRaMESH-sim-frozen-20260327
git checkout loramesh-sim-frozen-20260327
./ns3 configure
cmake --build cmake-cache -j$(nproc) --target scratch_LoRaMESH-sim_mesh_dv_baseline
```

Alternative from the bare repository copy:

```bash
git clone /path/to/LoRaMESH-sim-frozen-20260327.git LoRaMESH-sim-frozen-20260327
cd LoRaMESH-sim-frozen-20260327
git checkout loramesh-sim-frozen-20260327
./ns3 configure
cmake --build cmake-cache -j$(nproc) --target scratch_LoRaMESH-sim_mesh_dv_baseline
```

Notes:
- Use a fresh directory. Do not reuse an old `ns-3-dev` tree.
- The bundle and bare repo already contain the frozen tag.
- Current operational default profile is `pueyo2024_paper_like`.
