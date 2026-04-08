# Configurazione

## File .env

Il progetto usa un file `.env` (mai versionato) per le API key. Il file `.env.example` contiene le variabili necessarie:

```env
YOUTUBE_API_KEY=
NEWS_API_KEY=
GUARDIAN_API_KEY=
NYT_API_KEY=
```

## Come ottenere le API key

### YouTube Data API v3
1. Vai su [Google Cloud Console](https://console.cloud.google.com/)
2. Crea un progetto e abilita **YouTube Data API v3**
3. Vai su **Credenziali** → Crea chiave API

### NewsAPI
1. Registrati su [newsapi.org](https://newsapi.org/register)
2. La chiave è disponibile nella dashboard

### The Guardian
1. Registrati su [open-platform.theguardian.com/access](https://open-platform.theguardian.com/access/)
2. Seleziona **Developer key** (gratuito)
3. La chiave arriva via email in pochi minuti

### New York Times
1. Crea un account su [developer.nytimes.com](https://developer.nytimes.com/accounts/create)
2. Vai su **My Apps** → **New App**
3. Abilita **Article Search API** e copia la **Key**

## Fonti senza chiave

**GDELT DOC 2.0** e **Wikipedia** non richiedono registrazione né API key.
