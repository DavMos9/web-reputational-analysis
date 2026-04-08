# Installazione

## Requisiti

- Python 3.11 o superiore
- pip
- Git

## Passi

### 1. Clona la repository

```bash
git clone https://github.com/tuo-username/web-reputational-analysis.git
cd web-reputational-analysis
```

### 2. Crea un ambiente virtuale

```bash
python -m venv .venv
```

Attiva l'ambiente:

```bash
# macOS / Linux
source .venv/bin/activate

# Windows
.venv\Scripts\activate
```

### 3. Installa le dipendenze

```bash
pip install -r requirements.txt
```

### 4. Configura le variabili d'ambiente

```bash
cp .env.example .env
```

Apri `.env` e inserisci le tue API key. Vedi la pagina [Configurazione](Configurazione) per i dettagli.

## Verifica installazione

```bash
python main.py --help
```

Dovresti vedere l'elenco delle opzioni disponibili.
