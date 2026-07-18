#!/usr/bin/env python3
"""Fix the literal '\n' that ended up in the campaign csmacad scripts.
Replace it with a real newline so the shell continuation works."""
import os

REPO = "/home/diego/sim/LoRaMESH-sim-frozen-20260327"
FILES = [
    "campaign_launch_csmacad.sh",
    "campaign_random_equiv_csmacad.sh",
    "campaign_density_csmacad.sh",
]

# Pattern that's currently broken (literal '\n' as 2 chars):
BAD = '      --profile=pueyo2024_paper_like_csmacad     \\n      --rngRun="$seed"'
# What it should be (real newline + line-continuation backslash on previous line):
GOOD = '      --profile=pueyo2024_paper_like_csmacad     \\\n      --rngRun="$seed"'

for fname in FILES:
    path = os.path.join(REPO, fname)
    with open(path) as f:
        text = f.read()
    if BAD not in text:
        print(f"{fname}: bad pattern NOT found — already fixed?")
        # show what's around rngRun
        idx = text.find('--rngRun')
        if idx >= 0:
            print("  context:", repr(text[max(0, idx-80):idx+30]))
        continue
    new = text.replace(BAD, GOOD)
    with open(path, "w") as f:
        f.write(new)
    print(f"{fname}: fixed, {len(text)} -> {len(new)} bytes")
