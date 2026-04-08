# Schema Dati

Per la specifica completa vedere il [Data Contract PDF](../docs/Web_Reputational_Analysis_Data_Contract.pdf).

## Struttura del record

```json
{
  "source_type": "news",
  "source_name": "NewsAPI",
  "target_entity": "Giorgia Meloni",
  "query": "Giorgia Meloni governo",
  "title": "Titolo dell'articolo",
  "snippet": "Estratto breve...",
  "content": "Testo completo se disponibile",
  "url": "https://example.com/article",
  "domain": "example.com",
  "author": "Nome Autore",
  "published_at": "2026-04-08T10:00:00+00:00",
  "retrieved_at": "2026-04-08T16:12:18+00:00",
  "language": "it",
  "country": "IT",
  "rank": null,
  "views_count": null,
  "likes_count": null,
  "comments_count": null,
  "engagement_score": null,
  "keywords_found": [],
  "sentiment_stub": null,
  "raw_payload": {}
}
```

## Valori di `source_type`

| Valore | Fonte |
|---|---|
| `news` | NewsAPI, The Guardian, New York Times |
| `gdelt` | GDELT DOC 2.0 |
| `wikipedia` | Wikipedia |
| `youtube` | YouTube Data API |
| `reddit` | Reddit (PRAW) — in sviluppo |

## Campi obbligatori

Un record è valido solo se questi campi sono presenti e non vuoti:

- `source_type`
- `title`
- `url`
- `retrieved_at`

## Regole di normalizzazione

- **Date:** formato ISO 8601 (`2026-04-08T10:00:00+00:00`)
- **URL:** completi, con protocollo `https://`
- **Domain:** estratto automaticamente dall'URL
- **Stringhe:** spazi rimossi, encoding UTF-8 NFC
- **Valori mancanti:** `null` (non stringa vuota)
- **keywords_found in CSV:** serializzati come stringa con separatore `;`

## Deduplicazione

Due livelli applicati in sequenza:

1. **URL identico** — stesso URL dopo rimozione dei parametri di tracking (`utm_*`, `fbclid`, ecc.)
2. **Titolo + dominio** — stessa combinazione normalizzata (lowercase, punteggiatura rimossa)
