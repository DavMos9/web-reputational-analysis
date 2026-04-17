---
name: Feature Request
about: Suggerisci una nuova funzionalità o miglioramento
title: '[FEAT] '
labels: enhancement
assignees: ''
---

## Tipo di richiesta

- [ ] Nuovo collector (nuova fonte dati)
- [ ] Miglioramento collector esistente
- [ ] Nuovo processor (es. un passo aggiuntivo nella pipeline)
- [ ] Nuovo formato di export (es. JSONL, Parquet)
- [ ] Miglioramento CLI (nuovi parametri, output diverso)
- [ ] Altro

## Problema che vuoi risolvere

Descrivi l'esigenza concreta: es. "Con le fonti attuali non ottengo copertura di discussioni su forum e social", "Il deduplicatore non rileva articoli con titolo leggermente diverso ma contenuto identico".

## Soluzione proposta

Descrivi la funzionalità che vorresti: es. "Aggiungere un collector per Brave Search API che recupera risultati web generalisti", "Aggiungere un secondo livello di deduplicazione basato su similarità del titolo con soglia configurabile".

Se si tratta di un nuovo collector, specifica:
- **Nome della fonte:**
- **URL documentazione API:**
- **Richiede API key?**
- **Limite piano gratuito:**
- **Campi principali restituiti:** (es. titolo, URL, data, autore, score)

## Impatto sullo schema dati

Questa funzionalità richiede modifiche al Data Contract (nuovi campi, nuovi valori di `source_type`, ecc.)?

- [ ] No, compatibile con lo schema attuale
- [ ] Sì — descrivere: 

## Priorità stimata

- [ ] Bloccante — serve per completare un'analisi specifica
- [ ] Alta — migliora significativamente la qualità dei dati
- [ ] Media — utile ma non urgente
- [ ] Bassa — nice to have
