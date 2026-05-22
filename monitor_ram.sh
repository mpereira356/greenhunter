#!/bin/bash
# Script para monitorar uso de RAM e alertar se >80%

MEM_USAGE=$(free | grep Mem | awk '{printf "%.0f", $3/$2 * 100.0}')
if [ "$MEM_USAGE" -gt 80 ]; then
    echo "Alerta: RAM em $MEM_USAGE%" | mail -s "RAM Alta no Servidor" seuemail@example.com
fi