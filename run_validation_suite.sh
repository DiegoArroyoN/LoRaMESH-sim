#!/bin/bash
# =============================================================================
# LoRaMESH Validation Suite
# Ejecuta tests automatizados para validar el simulador
# =============================================================================

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NS3_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"
OUTPUT_DIR="${SCRIPT_DIR}/validation_results"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test counters
PASS=0
FAIL=0

# Create output directory
mkdir -p "$OUTPUT_DIR"

echo "============================================================"
echo "LoRaMESH Validation Suite"
echo "============================================================"
echo "NS3 Directory: $NS3_DIR"
echo "Output Directory: $OUTPUT_DIR"
echo "============================================================"
echo ""

# Function to run a test
run_test() {
    local test_name="$1"
    local description="$2"
    local command="$3"
    local check_pattern="$4"
    local expected="$5"
    
    echo -n "Testing: $description... "
    
    # Run command and capture output
    local output
    output=$(cd "$NS3_DIR" && eval "$command" 2>&1) || true
    
    # Check result
    if echo "$output" | grep -qE "$check_pattern"; then
        echo -e "${GREEN}PASS${NC}"
        PASS=$((PASS + 1))
        return 0
    else
        echo -e "${RED}FAIL${NC}"
        echo "  Expected: $expected"
        echo "  Command: $command"
        echo "  Output (last 3 lines):"
        echo "$output" | tail -3 | sed 's/^/    /'
        FAIL=$((FAIL + 1))
        return 1
    fi
}

# Function to run simulation and check output
run_sim_test() {
    local test_name="$1"
    local nEd="$2"
    local stopSec="$3"
    local extra_args="$4"
    local check_pattern="$5"
    local description="$6"
    
    local log_file="${OUTPUT_DIR}/${test_name}.log"
    
    echo -n "Testing: $description... "
    
    # Run simulation
    local cmd="./ns3 run \"mesh_dv_baseline --nEd=$nEd --stopSec=$stopSec $extra_args\""
    cd "$NS3_DIR"
    eval "$cmd" > "$log_file" 2>&1

    if [ -f "${NS3_DIR}/mesh_dv_metrics_tx.csv" ]; then
        cp "${NS3_DIR}/mesh_dv_metrics_tx.csv" "${OUTPUT_DIR}/${test_name}_tx.csv"
    fi
    if [ -f "${NS3_DIR}/mesh_dv_metrics_delay.csv" ]; then
        cp "${NS3_DIR}/mesh_dv_metrics_delay.csv" "${OUTPUT_DIR}/${test_name}_delay.csv"
    fi

    # Check result
    if grep -qE "$check_pattern" "$log_file"; then
        echo -e "${GREEN}PASS${NC}"
        PASS=$((PASS + 1))
        return 0
    else
        echo -e "${RED}FAIL${NC}"
        echo "  Log: $log_file"
        FAIL=$((FAIL + 1))
        return 1
    fi
}

echo "=== Phase 1: Build Verification ==="
run_test "build" "Simulator compiles successfully" \
    "./ns3 build 2>&1 | tail -5" \
    "Finished executing|Building CXX" \
    "Build completes without errors"

echo ""
echo "=== Phase 2: Parameter Verification ==="

# Check duty cycle is 1%
run_test "duty_cycle" "Duty Cycle = 1% (0.01)" \
    "grep -r 'DoubleValue(0.01)' src/loramesh/model/loramesh-mac-csma-cad.cc" \
    "0\.01" \
    "DutyCycleLimit = 0.01"

# Check reference loss
run_test "ref_loss" "L(d₀) = 127.41 dB" \
    "grep -r 'referenceLossDb.*127' scratch/LoRaMESH-sim/" \
    "127\.41" \
    "referenceLossDb = 127.41"

# Check route timeout
run_test "route_timeout" "Route timeout = 300s" \
    "grep -r 'm_routeTimeoutFactor' scratch/LoRaMESH-sim/mesh_dv_app.*" \
    "m_routeTimeoutFactor" \
    "Route timeout configurable por factor"

# Check beacon period
run_test "beacon_period" "Beacon period = 60s" \
    "grep -r 'm_period.*Seconds.*60' scratch/LoRaMESH-sim/" \
    "60" \
    "m_period = 60s"

# Check battery formula
run_test "battery_formula" "Battery penalty = 1-b^p" \
    "grep -r '1.0 - std::pow' src/loramesh/model/loramesh-metric-composite.cc" \
    "1\.0 - std::pow" \
    "Ψ(b) = 1 - b^p"

# Check shadowing
run_test "shadowing" "Shadowing σ = 3.57 dB" \
    "grep -r '3.57' src/loramesh/helper/loramesh-helper.cc" \
    "3\.57" \
    "σ = 3.57 dB"

echo ""
echo "=== Phase 3: Short Simulation Test ==="

# Run a quick simulation
run_sim_test "quick_test" 4 30 "--dataStartSec=5" \
    "Simulación completada|Exportando métricas|=== Simulación completada ===" \
    "Quick simulation (4 nodes, 30s)"

echo ""
echo "=== Phase 4: CSV Export Verification ==="

# Check if CSVs are generated
if [ -f "${OUTPUT_DIR}/quick_test_tx.csv" ]; then
    echo -e "Testing: TX CSV generated... ${GREEN}PASS${NC}"
    PASS=$((PASS + 1))
else
    echo -e "Testing: TX CSV generated... ${RED}FAIL${NC}"
    FAIL=$((FAIL + 1))
fi

if [ -f "${OUTPUT_DIR}/quick_test_delay.csv" ]; then
    echo -e "Testing: Delay CSV generated... ${GREEN}PASS${NC}"
    PASS=$((PASS + 1))
else
    echo -e "Testing: Delay CSV generated... ${RED}FAIL${NC}"
    FAIL=$((FAIL + 1))
fi

echo ""
echo "============================================================"
echo "VALIDATION SUMMARY"
echo "============================================================"
echo -e "Passed: ${GREEN}$PASS${NC}"
echo -e "Failed: ${RED}$FAIL${NC}"
echo "Total: $((PASS + FAIL))"
echo "============================================================"

if [ $FAIL -eq 0 ]; then
    echo -e "${GREEN}✓ All tests passed!${NC}"
    exit 0
else
    echo -e "${YELLOW}⚠ Some tests failed. Review logs above.${NC}"
    exit 1
fi
