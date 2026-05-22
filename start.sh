#!/bin/bash
# Script para iniciar o app com gunicorn, limitado a 2 workers para economizar RAM

python3 -m gunicorn --bind 0.0.0.0:8000 --workers 2 --threads 2 --max-requests 100 --max-requests-jitter 10 wsgi:app