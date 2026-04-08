# Utilizzo

## Sintassi base

```bash
python main.py --target "ENTITÀ" --queries "QUERY1" ["QUERY2" ...]
```

## Esempi

### Target singolo, query singola
```bash
python main.py --target "Giorgia Meloni" --queries "Giorgia Meloni"
```

### Target con query multiple
```bash
python main.py --target "Elon Musk" --queries "Elon Musk Tesla" "Elon Musk SpaceX" "Elon Musk DOGE"
```

### Solo alcune fonti
```bash
python main.py --target "Apple" --queries "Apple" --sources news gdelt guardian
```

### Senza salvare i raw (più veloce)
```bash
python main.py --target "Ferrari" --queries "Ferrari F1" --no-raw
```

### Più risultati per fonte
```bash
python main.py --target "OpenAI" --queries "OpenAI GPT" --max-results 50
```

## Parametri disponibili

| Parametro | Tipo | Default | Descrizione |
|---|---|---|---|
| `--target` | string | obbligatorio | Entità da analizzare |
| `--queries` | list | obbligatorio | Una o più query di ricerca |
| `--sources` | list | tutte | Fonti: `news gdelt wikipedia youtube guardian nyt` |
| `--max-results` | int | 20 | Risultati massimi per fonte/query |
| `--no-raw` | flag | False | Non salva i payload grezzi |

## Output

```
data/
├── raw/        ← payload originali delle API (per debug/audit)
├── processed/  ← record normalizzati e puliti
└── final/      ← JSON e CSV pronti per IBM Cloud Pak for Data
```

I file sono nominati con target e timestamp, es:
`giorgia_meloni_20260408T161218Z_final.json`
