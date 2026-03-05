-- ============================================================================
-- LoRaMESH Wireshark Dissector v1.3
-- ============================================================================
-- Parsea el header de 24 bytes y muestra campos en columnas de Wireshark.
-- Usa un DLT personalizado para evitar conflictos con IP.
--
-- INSTALACIÓN:
-- Windows: C:\Program Files\Wireshark\plugins\
-- Linux:   ~/.local/lib/wireshark/plugins/
-- ============================================================================

-- Evitar registración duplicada
if _G.loramesh_dissector_loaded then
    return
end
_G.loramesh_dissector_loaded = true

-- Crear el protocolo
local loramesh = Proto.new("loramesh", "LoRaMESH Protocol")

-- Definir campos del protocolo
local f_magic     = ProtoField.string("loramesh.magic", "Magic")
local f_node_id   = ProtoField.uint16("loramesh.node_id", "Observer Node", base.DEC)
local f_direction = ProtoField.string("loramesh.direction", "Direction")
local f_type      = ProtoField.string("loramesh.type", "Packet Type")
local f_wire      = ProtoField.string("loramesh.wire", "Wire Format")
local f_src       = ProtoField.uint16("loramesh.src", "Source", base.DEC)
local f_dst       = ProtoField.uint16("loramesh.dst", "Destination", base.HEX)
local f_via       = ProtoField.uint16("loramesh.via", "Via / Next Hop", base.DEC)
local f_seq       = ProtoField.uint32("loramesh.seq", "Sequence", base.DEC)
local f_seq16     = ProtoField.uint16("loramesh.seq16", "Sequence16", base.DEC)
local f_ttl       = ProtoField.uint8("loramesh.ttl", "TTL", base.DEC)
local f_rp_counter = ProtoField.uint8("loramesh.rp_counter", "Beacon RP Counter", base.DEC)
local f_hops      = ProtoField.uint8("loramesh.hops", "Hops", base.DEC)
local f_sf        = ProtoField.uint8("loramesh.sf", "Spreading Factor", base.DEC)
local f_rssi      = ProtoField.int16("loramesh.rssi", "RSSI (dBm)", base.DEC)
local f_battery   = ProtoField.uint16("loramesh.battery", "Battery (mV)", base.DEC)
local f_score     = ProtoField.uint16("loramesh.score", "Score (x100)", base.DEC)
local f_payload   = ProtoField.bytes("loramesh.payload", "Payload")
local f_dv_entries = ProtoField.uint16("loramesh.dv.entries", "DV Entries", base.DEC)
local f_dv_dst     = ProtoField.uint16("loramesh.dv.dst", "DV Destination", base.DEC)
local f_dv_hops    = ProtoField.uint8("loramesh.dv.hops", "DV Hops", base.DEC)
local f_dv_sf      = ProtoField.uint8("loramesh.dv.sf", "DV SF", base.DEC)
local f_dv_score   = ProtoField.uint8("loramesh.dv.score", "DV Score", base.DEC)
local f_dv_batt    = ProtoField.uint16("loramesh.dv.batt_mv", "DV Battery (mV)", base.DEC)

loramesh.fields = {
    f_magic, f_node_id, f_direction, f_type, f_wire, f_src, f_dst, f_via,
    f_seq, f_seq16, f_ttl, f_rp_counter, f_hops, f_sf, f_rssi, f_battery, f_score, f_payload,
    f_dv_entries, f_dv_dst, f_dv_hops, f_dv_sf, f_dv_score, f_dv_batt
}

-- Función principal de disección
function loramesh.dissector(buffer, pinfo, tree)
    if buffer:len() < 24 then
        return 0
    end
    
    -- Verificar magic bytes "MS" (0x4D 0x53)
    local magic1 = buffer(0, 1):uint()
    local magic2 = buffer(1, 1):uint()
    
    if magic1 ~= 0x4D or magic2 ~= 0x53 then
        return 0
    end
    
    -- Extraer campos
    local node_id = buffer(2, 2):le_uint()
    local direction_byte = buffer(4, 1):uint()
    local type_byte = buffer(5, 1):uint()
    local src = buffer(6, 2):le_uint()
    local dst = buffer(8, 2):le_uint()
    local seq = buffer(10, 4):le_uint()
    local ttl = buffer(14, 1):uint()
    local hops = buffer(15, 1):uint()
    local sf = buffer(16, 1):uint()
    local rssi = buffer(17, 2):le_int()
    local battery = buffer(19, 2):le_uint()
    local score = buffer(21, 2):le_uint()
    
    -- Interpretar dirección
    local direction_str = "?"
    if direction_byte == 0x54 then
        direction_str = "TX"
    elseif direction_byte == 0x52 then
        direction_str = "RX"
    end
    
    -- Interpretar tipo
    local type_str = "?"
    if type_byte == 0x42 then
        type_str = "Beacon"
    elseif type_byte == 0x55 then
        type_str = "Data"
    elseif type_byte == 0x3F then
        type_str = "Unknown"
    end
    
    -- Interpretar destino para display
    local dst_str = tostring(dst)
    if dst == 0xFFFF then
        dst_str = "BCAST"
    end

    -- ========================================================================
    -- COLUMNAS DE WIRESHARK
    -- ========================================================================
    pinfo.cols.protocol:set("LoRaMESH")
    pinfo.cols.src:set(string.format("Node %d", src))
    pinfo.cols.dst:set(dst == 0xFFFF and "BROADCAST" or string.format("Node %d", dst))
    
    local score_pct = math.min(score, 100)
    pinfo.cols.info:set(string.format("[%s] %s Seq=%d H=%d SF%d RSSI=%d Score=%d%%",
        direction_str, type_str, seq, hops, sf, rssi, score_pct))
    
    -- ========================================================================
    -- ÁRBOL DE DETALLES - Reemplazar el árbol existente
    -- ========================================================================
    -- Limpiar cualquier disección previa agregando nuestro árbol primero
    local subtree = tree:add(loramesh, buffer(0, buffer:len()), "LoRaMESH Protocol")
    
    -- Header section
    local header = subtree:add(loramesh, buffer(0, 24), "Header (24 bytes)")
    header:add(f_magic, buffer(0, 2), "MS")
    header:add_le(f_node_id, buffer(2, 2)):append_text(string.format(" (Observer: Node %d)", node_id))
    header:add(f_direction, buffer(4, 1), direction_str)
    header:add(f_type, buffer(5, 1), type_str)
    header:add_le(f_src, buffer(6, 2)):append_text(string.format(" (Node %d)", src))
    
    local dst_node = header:add_le(f_dst, buffer(8, 2))
    if dst == 0xFFFF then
        dst_node:append_text(" (BROADCAST)")
    else
        dst_node:append_text(string.format(" (Node %d)", dst))
    end
    
    header:add_le(f_seq, buffer(10, 4))
    header:add(f_ttl, buffer(14, 1))
    header:add(f_hops, buffer(15, 1))
    header:add(f_sf, buffer(16, 1)):append_text(string.format(" (SF%d)", sf))
    header:add_le(f_rssi, buffer(17, 2)):append_text(" dBm")
    header:add_le(f_battery, buffer(19, 2)):append_text(" mV")
    header:add_le(f_score, buffer(21, 2)):append_text(string.format(" (%d%%)", score_pct))
    
    -- Payload section
    if buffer:len() > 24 then
        local payload_len = buffer:len() - 24
        subtree:add(f_payload, buffer(24, payload_len)):append_text(string.format(" (%d bytes)", payload_len))

        if type_byte == 0x42 then
            -- Beacon payload: support legacy v1 and compact wire v2.
            local l2_offset = 24
            local mesh_hdr_len = 12
            local wire_fmt = "v1-legacy"
            local dv_offset = l2_offset + mesh_hdr_len
            local entry_size = 6

            -- Heuristic for v2 beacon on-air:
            -- [src(2) dst(2=0xFFFF) flags_ttl(1)] + N*[dst(2) score(1)]
            if payload_len >= 5 then
                local b_src = buffer(24, 2):le_uint()
                local b_dst = buffer(26, 2):le_uint()
                local b_flags = buffer(28, 1):uint()
                local b_type = bit32.rshift(bit32.band(b_flags, 0xC0), 6)
                if b_dst == 0xFFFF and b_type == 1 then
                    wire_fmt = "v2"
                    dv_offset = 24 + 5
                    entry_size = 3
                    local btree = subtree:add(loramesh, buffer(24, 5),
                        string.format("Beacon Wire v2 Header: src=%d dst=BCAST flags=0x%02X", b_src, b_flags))
                    btree:add(f_wire, wire_fmt)
                    btree:add_le(f_src, buffer(24, 2))
                    btree:add_le(f_dst, buffer(26, 2))
                    btree:add(f_rp_counter, bit32.band(b_flags, 0x3F))
                end
            end

            local dv_len = buffer:len() - dv_offset
            if dv_len >= entry_size then
                local n_entries = math.floor(dv_len / entry_size)
                local dv_tree = subtree:add(loramesh, buffer(dv_offset, n_entries * entry_size),
                    string.format("DV Payload %s (%d entries)", wire_fmt, n_entries))
                dv_tree:add(f_wire, wire_fmt)
                dv_tree:add(f_dv_entries, n_entries)

                for i = 0, n_entries - 1 do
                    local o = dv_offset + i * entry_size
                    if wire_fmt == "v2" then
                        local dst_dv = buffer(o, 2):le_uint()
                        local score_dv = buffer(o + 2, 1):uint()
                        local e = dv_tree:add(loramesh, buffer(o, entry_size),
                            string.format("Entry %d: dst=%d score=%d", i + 1, dst_dv, score_dv))
                        e:add_le(f_dv_dst, buffer(o, 2))
                        e:add(f_dv_score, buffer(o + 2, 1))
                    else
                        local dst_dv = buffer(o, 2):le_uint()
                        local hops_dv = buffer(o + 2, 1):uint()
                        local sf_dv = buffer(o + 3, 1):uint()
                        local score_dv = buffer(o + 4, 1):uint()
                        local batt_q = buffer(o + 5, 1):uint()
                        local batt_mv = math.floor((batt_q * 5000.0 / 255.0) + 0.5)
                        local e = dv_tree:add(loramesh, buffer(o, entry_size),
                            string.format("Entry %d: dst=%d hops=%d sf=%d score=%d batt=%dmV",
                                i + 1, dst_dv, hops_dv, sf_dv, score_dv, batt_mv))
                        e:add_le(f_dv_dst, buffer(o, 2))
                        e:add(f_dv_hops, buffer(o + 2, 1))
                        e:add(f_dv_sf, buffer(o + 3, 1))
                        e:add(f_dv_score, buffer(o + 4, 1))
                        e:add(f_dv_batt, batt_mv)
                    end
                end
            end
        elseif type_byte == 0x55 and payload_len >= 9 then
            -- Data on-air v2: src(2),dst(2),via(2),flags_ttl(1),seq16(2)
            local d_src = buffer(24, 2):le_uint()
            local d_dst = buffer(26, 2):le_uint()
            local d_via = buffer(28, 2):le_uint()
            local d_flags = buffer(30, 1):uint()
            local d_seq16 = buffer(31, 2):le_uint()
            local d_type = bit32.rshift(bit32.band(d_flags, 0xC0), 6)
            if d_type == 0 then
                local dtree = subtree:add(loramesh, buffer(24, 9),
                    string.format("Data Wire v2 Header: src=%d dst=%d via=%d seq16=%d", d_src, d_dst, d_via, d_seq16))
                dtree:add(f_wire, "v2")
                dtree:add_le(f_src, buffer(24, 2))
                dtree:add_le(f_dst, buffer(26, 2))
                dtree:add_le(f_via, buffer(28, 2))
                dtree:add(f_ttl, bit32.band(d_flags, 0x3F))
                dtree:add_le(f_seq16, buffer(31, 2))
            end
        end
    end
    
    return buffer:len()
end

-- ============================================================================
-- Registrar para el DLT NULL (link-type 0) usado por ns-3
-- ============================================================================
local wtap_encap_table = DissectorTable.get("wtap_encap")
wtap_encap_table:add(wtap.NULL, loramesh)

-- También registrar para "data" genérico como fallback
local data_dissector_table = DissectorTable.get("null.type")
if data_dissector_table then
    -- Registrar para tipo 0x534D ("MS" en big-endian interpretado como family)
    data_dissector_table:add(0x534D, loramesh)
    data_dissector_table:add(0x4D53, loramesh)
end

print("LoRaMESH Dissector v1.3 loaded - registered for NULL encapsulation")
