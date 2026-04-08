# Web Reputational Analysis — Wiki

Benvenuto nella documentazione del progetto.

## Pagine disponibili

| Pagina | Descrizione |
|---|---|
| [Installazione](Installazione) | Setup dell'ambiente e delle dipendenze |
| [Configurazione](Configurazione) | API key e variabili d'ambiente |
| [Utilizzo](Utilizzo) | Comandi CLI ed esempi pratici |
| [Architettura](Architettura) | Pipeline, moduli e flusso dati |
| [Collectors](Collectors) | Documentazione di ogni fonte dati |
| [Schema Dati](Schema-Dati) | Riferimento completo del data contract |

## Quick start

```bash
git clone https://github.com/tuo-username/web-reputational-analysis.git
cd web-reputational-analysis
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # inserisci le tue chiavi
python main.py --target "Nome Target" --queries "query principale"
```

## Riferimenti

- [Documentazione di Progetto (PDF)](../docs/Web_Reputational_Analysis_Project.pdf)
- [Data Contract (PDF)](../docs/Web_Reputational_Analysis_Data_Contract.pdf)
