# Resultado do benchmark paralelo

- Eventos: 600,000
- Repetições medidas: 5
- Python: 3.13.4
- CPUs lógicas: 32

| Modo | Processos | Mediana (s) | p95 (s) | Desvio (s) | Speedup | Eficiência |
|---|---:|---:|---:|---:|---:|---:|
| sequential | 0 | 1.6024 | 1.6107 | 0.0390 | 1.00x | 100.0% |
| parallel | 1 | 1.8402 | 1.8885 | 0.0657 | 0.87x | 87.1% |
| parallel | 2 | 1.0469 | 1.1079 | 0.0646 | 1.53x | 76.5% |
| parallel | 4 | 0.9615 | 1.0512 | 0.1066 | 1.67x | 41.7% |
| parallel | 6 | 0.7153 | 1.1664 | 0.2504 | 2.24x | 37.3% |
