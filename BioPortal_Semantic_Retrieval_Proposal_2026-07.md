# Von lexikalischem zu tatsächlich semantischem Retrieval gegen BioPortal

*Erstellt: 2026-07-05 · Fokus: Warum der semantisch beste Ontologieterm oft **gar nicht erst zum Kandidaten** wird, und wie man — gegeben was die BioPortal/OntoPortal-API kann und nicht kann — eine **echte semantische** statt nur lexikalischen Abfrage macht. Nur Vorschläge, kein Code geändert.*

Alle Änderungen sind **additiv**: jeder neue Retriever emittiert `AgentCandidate`-Objekte in den bestehenden `raw_candidate_scores`-Pool und erbt die aktuelle Kaskade (dedupe → prerank → truncate → top-3 enrich → LLM-Adjudikation) unverändert.

**Verifizierter Ist-Retriever:** [`search_bioportal_candidates`](agentic_reconciliation/agent_bioportal_service.py:354) ist der *einzige* Kandidaten-Retriever. Er sendet **einen** `GET {base}/search` mit `q=<Term wortwörtlich>`, `include=prefLabel,synonym,definition,notation,cui,semanticType`, `page=1`, `pagesize=<n>`, `display_context=false`, `display_links=true`, optional `ontologies=<csv>`. Er setzt **nicht** `require_exact_match`, `also_search_properties`, `also_search_obsolete`, `also_search_views`, `suggest`, `semantic_types`, `cui`, `subtree_root_id` oder `language`, und holt nur Seite 1. Das `/annotator`-Endpoint ist **ungenutzt**; `/recommender` nur fürs Routing. Es gibt **kein** Embedding/Vektor-Index im Paket.

Verifizierte Defaults: `per_ontology_candidate_limit=3`, `global_candidate_pool_limit=20`, `shortlist_context_limit=3` (nur Top-3 werden angereichert).

---

## A. Warum die besten Treffer nie abgerufen werden

Der korrekte Term wird aus **vier mechanistisch verschiedenen Gründen** nicht zum Kandidaten. Verschieden, weil jeder eine *andere* Lösung braucht — deshalb funktioniert „einfach die Query anpassen" nicht.

### Grund 1 — Vokabular-Mismatch: Query und korrektes Label teilen keine Oberflächen-Tokens (der dominante Fall)
BioPortals `/search` ist **Apache Solr** mit `defType=edismax`. Im Default-Zweig (kein `require_exact_match`, kein `suggest`) sind die Query-Felder (laut BioPortal-Quellcode ncbo/ontologies_api, `helpers/search_helper.rb`):

```
qf   = resource_id^100 notation^100 oboId^100 prefLabelExact^90 prefLabel^70 synonymExact^50 synonym^10
bq   = idAcronymMatch:true^80
boost= sum(ontologyRank,1)
```

`prefLabel`/`synonym` nutzen den Solr-Typ `text_general`: `StandardTokenizer` + `StopFilter` + `LowerCase` mit query-seitigem `SynonymGraphFilter` über eine kuratierte `synonyms.txt`. Entscheidend: **Wort-Tokenisierung + Lowercasing, KEIN Stemming** (das Schema hat `text_en` mit Porter-Stemmer, aber der ist **nicht** an `prefLabel`/`synonym` gehängt). Matching ist **Token-Match**, kein Substring/contains. Ranking ist BM25/TF-IDF × `(ontologyRank+1)`.

Konsequenz: um überhaupt Kandidat zu werden, muss `prefLabel` oder ein `synonym` des Konzepts **analysierte Tokens mit der Query teilen** (oder über `synonyms.txt` verknüpft sein). Deshalb erscheinen — egal wie „offensichtlich richtig" — **nie**:
- **Paraphrasen** — „heart attack" holt keine Klasse „myocardial infarction", außer „heart attack" ist als Synonym eingetragen.
- **Nicht schon in der Query stehende Abkürzungen** — „MI" erreicht „myocardial infarction" nicht.
- **Morphologische Abweichung jenseits von Token-Identität** — ohne Stemming matchen flektierte Formen nur, wenn sie zufällig identisch tokenisieren.

Das ist das klassische **Vokabular-Mismatch-Problem** und strukturell: eine Token-Matching-Engine kann zwei token-disjunkte, gleichbedeutende Strings nicht überbrücken.

### Grund 2 — Sprach-Mismatch: deutsche Query vs. englisches Label
Der Retriever setzt nie `language` → BioPortal nutzt Englisch (`prefLabel`, nicht `prefLabel_de`). Ein deutscher Quellterm (dies ist ein deutschsprachiges Toolkit) ist ein *garantierter* token-disjunkter Miss gegen ein englisches `prefLabel`, außer die Ontologie führt zufällig ein literal passendes deutsches Synonym. Der Wikidata-Fallback verschärft es: er kodiert `lang="en"` hart. Sprach-Mismatch ist ein *schwerer Spezialfall* von Grund 1 (per Konstruktion null Token-Overlap) und braucht eine eigene Lösung (Übersetzung oder multilinguales Modell).

### Grund 3 — Die Bedeutung steckt nur in der *Definition/Property*, nicht im Label — aber die Felder werden nicht durchsucht
`prefLabel`/`synonym` des richtigen Konzepts überlappen die Query evtl. nicht, aber seine **Definition/Property-Texte** beschreiben die Bedeutung exakt. Das Default-`qf` matcht nur `prefLabel`/`synonym` (+`resource_id/notation/oboId`). BioPortal indiziert ein `property`-Feld (aus `propertyRaw` — Labels, Kommentare, Annotation-/Property-Werte) und bietet **`also_search_properties=true`**, um es ins `qf` aufzunehmen — **aber der Retriever setzt es nie.** Eine Klasse, deren *Bedeutung* im Gloss steht, deren *Name* aber token-disjunkt ist, ist unsichtbar. Ebenso hängt die Default-Filter-Query `AND obsolete:false` und `AND -provisional:true` an (blendet still Deprecated/Proposed aus); `also_search_obsolete=true`/`also_search_provisional=true` würden sie re-inkludieren — nicht gesetzt.

### Grund 4 — Coverage-/Ranking-Cutoffs verwerfen Konzepte, die die Lexik-Engine *rankte*
Selbst wenn Solr die richtige Klasse zurückgäbe, kappen zwei Cutoffs sie vor der Adjudikation:
- **Einzelseite, kleines `pagesize`.** Der Retriever holt nur `page=1`. Per-Ontologie-Calls nutzen `pagesize=3` (`per_ontology_candidate_limit`), Broad-Calls ~20–30. Ein Konzept auf Rang #4 einer gerouteten Ontologie oder #35 global wird nie gesehen. Keine Pagination.
- **Der 5-Ontologien-Routing-Cap** ([agent_ontology_routing.py:2171](agentic_reconciliation/agent_ontology_routing.py:2171)) — liegt das beste Konzept in der 6.-besten (oder einer fehl-gerankten) Ontologie, ist es out-of-scope; `/search` filtert per `{!terms f=submissionAcronym}` auf die abgefragten Ontologien.
- **Nur wortwörtlich, keine Query-Variation auf dem Hauptpfad.** Alle 8+ Retrieval-Legs (`agent_orchestrator_workflows.py` Zeilen 3091, 3186, 3268, 3306, 3405, 3503, 3767) rufen dasselbe `search_bioportal_candidates` mit `q=<verbatim>` — Fallbacks variieren die *Ontologie-Menge*, nie den *Query-String*. Die einzige echte Expansion ([`_extra_search`](agentic_reconciliation/agent_orchestrator_workflows.py:255) + [`derive_abbreviation_expansions`](agentic_reconciliation/agent_rescue_adjudication.py:207)/`derive_definition_queries`) läuft **nur im teuren Rescue-Pass**, braucht einen schon existierenden Kandidatenpool (Abkürzungs-Expansion bootstrappt aus bereits gefundenem Kandidatentext — Henne-Ei), und schreibt in einen rescue-lokalen `merged`-Pool, **nicht** in `raw_candidate_scores`. Zudem ist sie eine reine String-Heuristik (kein LLM, keine Übersetzung; nur einzelne Tokens, `if " " in raw: return []`).

**Zusammenfassung A:** Gründe 1–2 = *Retrieval-Mechanismus* (Token/Sprache — die Engine sieht das Konzept nicht). Grund 3 = *Feld-Coverage* (richtige Bedeutung, falsches durchsuchtes Feld). Grund 4 = *Cutoff* (richtiges Konzept gerankt, aber gekappt/out-of-scope). **Tier 0** behebt 3 + Teil von 4. **Tier 1–2** greifen 1–2 pseudo-semantisch an. **Tier 3** greift 1–2 mit echter Semantik an.

---

## B. Was „semantisch" gegen BioPortals API bedeuten kann — und was nicht

**BioPortals `/search` hat KEINE Vektor-/Embedding-/Ähnlichkeitssuche.** Bestätigt in der [offiziellen Doku](https://data.bioontology.org/documentation) und im Quellcode (`search_helper.rb`): die Query ist *immer* eine lexikalische Solr-`edismax`-Query über Oberflächen-Token-Felder. `suggest=true` ist **auch nicht** semantisch — es geht auf EdgeNGram-Autocomplete-Felder (Präfix/Substring). `semantic_types` ist ein UMLS-TUI-*Kategoriefilter*, `cui` ein exakter UMLS-Code-Join.

„Echtes semantisches Retrieval" kann also nur aus einem (oder beiden) kommen:

1. **Mehr von der BioPortal-Fläche ausnutzen** für *pseudo-semantischen* Recall komplett in-API: `also_search_properties` (Definitionen/Properties durchsuchen — Grund 3), das **`/annotator`**-Endpoint (Dictionary-Konzepterkennung, die Input *segmentiert* und Definitionstext minet), per-class **`/mappings`** (`LOOM`/`CUI`/`SAME_URI`/`REST`) zur Expansion eines Lexik-Treffers auf seine ontologieübergreifende Äquivalenzmenge, **UMLS-CUI-Atome** für vokabularübergreifende Synonyme. Das hebt Recall über Token-/Feld-/Scope-Lücken, kann aber **keine Bedeutung überbrücken, die kein Oberflächenstring/keine kuratierte Mapping abdeckt** — „heart attack"→„myocardial infarction" braucht den String irgendwo als Synonym/Mapping.
2. **Eine externe Embedding-Schicht, geseedet aus BioPortal-Daten.** Corpus (`prefLabel`+Synonyme+Definition) über `/ontologies/{acronym}/classes` oder `/download` holen, mit **multilingualem biomedizinischem** Modell embedden, in FAISS/Qdrant indexieren, per Cosinus-ANN abrufen. Das ist der *einzige* Weg zu **echtem** semantischem Recall — Bedeutung unabhängig von geteilten Tokens **und** unabhängig von der Sprache (deutsche Query → englisches Label).

**Fazit:** `/search` liefert starken **lexikalischen** Recall und, mit den richtigen Parametern, brauchbare **pseudo-semantische** Reichweite; **echter** semantischer Recall braucht eine zusätzliche Expansions- oder Embedding-Schicht.

---

## C. Gestufte Lösungsvorschläge

Alle Tiers docken an dieselbe Naht an. Das überall genutzte Idiom (Vorlage [agent_orchestrator_workflows.py:3112](agentic_reconciliation/agent_orchestrator_workflows.py:3112)):

```python
for rank, candidate in enumerate(new_retriever(term, ...)):        # liefert AgentCandidate
    raw_candidate_scores.append(
        _cheap_prerank_score(term, definition, candidate, effective_config, api_rank=rank)
    )   # optional: CandidateScore.from_fallback = True
```

`raw_candidate_scores` wird bei ~3016 initialisiert, einmalig dedupliziert/gerankt bei [~3353–3364](agentic_reconciliation/agent_orchestrator_workflows.py:3353). **Sauberstes Einfügefenster:** nach den Bulk-Legs, vor dem einmaligen Dedupe (~3127–3242). `AgentCandidate`-Form: das Mapping [agent_bioportal_service.py:384-401](agentic_reconciliation/agent_bioportal_service.py:384) (min.: `uri`, `label`, `description`, `source_provider`, `ontology_context.ontology_acronym`). **Überlebens-Caveat:** ein Kandidat muss den Low-Confidence-Filter bei [~3374–3380](agentic_reconciliation/agent_orchestrator_workflows.py:3374) passieren (`mapping_type != 'none'` ODER `combined_confidence >= 0.10` ODER `_candidate_is_plausible`) — ein reiner Semantik-Treffer braucht plausible Anreicherung, um zu überleben.

### Tier 0 — BioPortals eigenes `/search` stärker ausreizen — **zuerst machen**
Greift Grund 3 + Teil von Grund 4 durch Parameter am *bestehenden* Call. Keine neue Komponente; Edit in [agent_bioportal_service.py:361-371](agentic_reconciliation/agent_bioportal_service.py:361).

| Param | Wert | Behebt | Guardrail |
|---|---|---|---|
| `also_search_properties` | `true` | Grund 3 — Definitions-/Property-Text | Präzision ↓; als *ergänzendes* Leg oder Property-Treffer down-weighten |
| `also_search_obsolete` | `true` (optional) | Grund 4 — Legacy-Terme | Kann Deprecated liefern; flaggen, nicht auto-akzeptieren |
| `language` / `lang` | Quellsprache (oder `all`) | Grund 2 — `prefLabel_<lang>` | Präzisionsneutral; reiner Recall-Hebel |
| Pagination | `page=1..K` **oder** `pagesize↑` | Grund 4 — tief gerankte Treffer | Mehr Rauschen → Prerank/Adjudikation fängt es; `K` deckeln |
| `suggest` | `true` (separates Leg) | Präfix-/Fragment-Terme | Nur Autocomplete-Semantik; für Fragmente |

**Beispiel-Request (Property + Sprache + tiefere Seite):**
```
GET https://data.bioontology.org/search
  ?q=malignes%20Melanom
  &include=prefLabel,synonym,definition,notation,cui,semanticType
  &also_search_properties=true
  &language=de
  &page=1&pagesize=50
  &ontologies=NCIT,MONDO,DOID,SNOMEDCT,MESH
  &display_context=false&display_links=true
  &apikey=YOUR_KEY
```
Optional `lexical_match_type="property"` im `ontology_context` setzen, damit Downstream Property- von Label-Treffern unterscheiden kann.
- **Recall:** moderat–hoch für Grund 3/4; echter Sprachgewinn, falls deutsche Felder existieren. **Präzision:** Rauschen absorbiert die bestehende Prerank+≤6-Adjudikation. **Aufwand: S** (Stunden–1 Tag). **Infra/Latenz/$:** nichts Neues; +K× Calls bei Pagination (Ratenlimit **15 req/s** pro Key). **Abhängigkeiten:** keine.

### Tier 1 — BioPortal Annotator als zweiter Retriever (pseudo-semantisch, in-API)
`/annotator` erkennt Ontologiekonzepte *im* Freitext via Dictionary + Mgrep-Longest-Match über alle Klassen-`prefLabel`+Synonyme. Anders als `/search` (rankt ganze Klassen gegen den ganzen Query-String) **segmentiert** Annotator den Input und findet so (a) Mehrwort-Bestandteile, (b) in längeren Strings eingebettete Konzepte, und — der Schlüssel — (c) im **Definitionstext des Terms** erwähnte Konzepte. `expand_mappings=true` zieht ontologieübergreifende Äquivalente, `expand_class_hierarchy=true` Eltern. Pseudo-semantisch: weiterhin Dictionary/lexikalisch, englisch-orientiert, keine Paraphrase — aber erreicht Konzepte, die `/search` strukturell verpasst.

**Zwei Pässe pro Term:**
```
# Pass 1 — Term annotieren (Mehrwort/längste Spanne)
GET https://data.bioontology.org/annotator
  ?text=malignant%20melanoma%20of%20skin
  &ontologies=NCIT,SNOMEDCT,MESH
  &longest_only=true&whole_word_only=true&exclude_numbers=true
  &apikey=YOUR_KEY

# Pass 2 — Definition annotieren (eingebettete Konzepte + Expansion)
GET https://data.bioontology.org/annotator
  ?text=Melanoma%20is%20a%20malignant%20tumor%20of%20melanocytes%20found%20in%20skin.
  &ontologies=NCIT,SNOMEDCT
  &expand_mappings=true&expand_class_hierarchy=true&class_hierarchy_max_level=2
  &longest_only=true
  &apikey=YOUR_KEY
```
**Plug-in:** neues `annotator_retriever(term, definition, ontologies, api_key)`, das jede `annotatedClass` (`@id` + Ontologie-Link) auf einen `AgentCandidate` mappt (spiegelt 384-401), dann das Standard-Idiom im Fenster 3127–3242.
- **Recall:** deutlich für Mehrwort-Terme + in Definitionen genannte Konzepte; `expand_mappings` reicht über den 5-Ontologien-Cap hinaus. **Präzision:** `ontologies` einschränken (all-ontology ist langsam/rauschig); `longest_only=true`; Mapping/Hierarchie-Expansionen als `from_fallback=True` markieren. **Aufwand: M** (2–4 Tage). **Latenz:** +2 Calls/Term (parallelisieren). **Ratenlimit:** 15 req/s.
- *Optional:* NCBO **Annotator+** (`services.bioportal.lirmm.fr/ncbo_annotatorplus`) mit `lemmatize=true` (morphologische Normalisierung, z.B. `lesions`→`lesion`) + C-Value-`score`/`score_threshold` — [Tchechmedjiev et al. 2018, Bioinformatics 34(11):1962](https://academic.oup.com/bioinformatics/article/34/11/1962/4802221).

### Tier 2 — Semantische Query-Expansion (schließt die Deutsch- + Paraphrasen-Lücke, ohne Vektor-DB)
`/search`+`/annotator` bleiben Retriever, aber die einzelne wortwörtliche Query wird durch eine **Union von Varianten** ersetzt: LLM-/Lexikon-generierte Synonyme, Paraphrasen, Abkürzungs-Expansionen und **DE→EN-Übersetzung**. Die *semantische* Arbeit wandert in den Variantengenerator; die Lexik-Engine macht dann schnelles Token-Matching über viele Oberflächenformen derselben Bedeutung. Query-seitiges Dual von doc2query, LLM-Dual von HyDE, validiert für Ontology-Matching durch GenOM. ([HyDE arXiv:2212.10496](https://arxiv.org/abs/2212.10496); [doc2query arXiv:1904.08375](https://arxiv.org/pdf/1904.08375); [GenOM arXiv:2508.10703](https://arxiv.org/abs/2508.10703).)

**Variantenquellen:** LLM (Synonyme/Paraphrasen/Abk./DE→EN, ~≤6 Varianten); **UMLS UTS** (stärkster kuratierter Hebel: Term→CUI, dann `GET /rest/content/current/CUI/{cui}/atoms?sabs=...` liefert Synonyme über SNOMED/MeSH/NCIt — [UTS-Doku](https://documentation.uts.nlm.nih.gov/rest/atoms/), 20 req/s/IP, UTS-Lizenz); Wikidata (das harte `lang="en"` fallenlassen → deutsche Aliase). Die bestehenden [`derive_abbreviation_expansions`](agentic_reconciliation/agent_rescue_adjudication.py:207)/`derive_definition_queries` (heute rescue-only) auf den **Hauptpfad** heben.

```
for v in expand(term):   # z.B. ["malignant melanoma","malignant melanoma of skin","MM","malignes Melanom"]
    GET https://data.bioontology.org/search?q=<v>&include=...&also_search_properties=true
        &page=1&pagesize=30&ontologies=NCIT,MONDO,DOID,SNOMEDCT&apikey=YOUR_KEY
# Union + Dedupe nach @id → AgentCandidate → _cheap_prerank_score → raw_candidate_scores
```
**Plug-in:** `search_bioportal_candidates` in `expanded_search_retriever` wrappen, Varianten loopen, nach `@id` deduplizieren ([`_candidate_key`:905](agentic_reconciliation/agent_orchestrator_workflows.py:905)), via Standard-Idiom appenden.
- **Recall: groß** für die zwei größten Blind Spots — cross-lingual (Grund 2) + Abk./Paraphrase (Grund 1) — bei **null neuer Infra**. **Präzision:** falsche Expansionen driften; **Varianten ≤6 deckeln**, kuratiert (UMLS/Wikidata) vor freiem LLM bevorzugen, ≤6-Adjudikation filtert Drift; Varianten pro Term cachen. **Aufwand: S–M** (2–5 Tage). **Latenz:** N Calls/Term → parallelisieren + cachen. **$:** LLM-Varianten-Tokens; optional UTS-Key.

### Tier 3 — Externer Embedding-Index (der echte semantische Retriever)
Einziger Weg zu **echtem** semantischem Recall: Bedeutung unabhängig von geteilten Tokens *und* Sprache matchen, inkl. Paraphrasen, die keine Variante aufzählt. Retrieve-then-rerank (BLINK, GenOM). ([BLINK arXiv:1911.03814](https://arxiv.org/abs/1911.03814).)

**Build (offline):**
1. Corpus der **gerouteten** Ontologien (nicht ganz BioPortal) holen: `GET /ontologies/{acronym}/download?download_format=csv` (schnellste) oder paged `GET /ontologies/{acronym}/classes?include=prefLabel,synonym,definition,notation,cui,semanticType&pagesize=500&page=1..` bis `links.nextPage` null. (15 req/s.)
2. **Granularität: ein Vektor pro Synonym** (+ `prefLabel` + `definition`), nicht ein gemittelter pro Konzept — so feuert ein Match auf *jede* Oberflächenform (SapBERT-Design, [arXiv:2010.11784](https://arxiv.org/abs/2010.11784)).
3. **Modell (multilingual + biomedizinisch, für DE↔EN):** **BGE-M3** (1024-dim, 100+ Sprachen — praktischer Default) oder **BioLORD-2023-M** (768-dim, definitionstrainiert); englisch-präzise Alternative **SapBERT** (768-dim). ([BGE-M3 arXiv:2402.03216](https://arxiv.org/abs/2402.03216); [multilingual-e5 arXiv:2402.05672](https://arxiv.org/abs/2402.05672); [LaBSE arXiv:2007.01852](https://arxiv.org/abs/2007.01852); [BioLORD-2023 arXiv:2311.16075](https://arxiv.org/abs/2311.16075).)
4. FAISS `IndexHNSWFlat(d, M=32)`, `efConstruction≈200`, Cosinus auf L2-normalisierten Vektoren. **Sizing:** ~100k Konzepte × ~2 Synonyme ≈ 200k Vektoren; d=1024 ≈ **~0,85 GB RAM** (d=768 ≈ ~0,6 GB). HNSW-Benchmarks: `M=32, efSearch=128` → recall@10 ≈0,97 bei p95 ≈8 ms.

```python
# Query (online)
qv = model.encode(term)                 # multilingual: deutscher Term embeddet direkt
ids, scores = index.search(qv, k=50)    # großzügiges k — das ist das Recall-Leg
# concept_id → AgentCandidate (Label/Def via POST /batch oder lokalen Cache hydrieren)
```
**Hybrid-Fusion + Rerank (beste Qualität):** lexikalisches Leg (Tier 0/2) **und** dichtes Leg laufen lassen, Ranglisten per **Reciprocal Rank Fusion** fusionieren (`score(d)=Σ 1/(60+rank_i(d))` — keine Score-Normalisierung, zero-tuning), fusionierte Top 50–200 mit multilingualem Cross-Encoder **bge-reranker-v2-m3** reranken, dann ein **UMLS-Typ/Domänen-Gate** (Kandidaten mit inkompatiblem `semanticType`/TUI oder Quellontologie droppen — nutzt das schon in `include` vorhandene `semanticType`, oder Upstream `semantic_types=`/`cui=`) gegen dichte „false friends". ([RRF, Cormack et al. SIGIR 2009](https://dl.acm.org/doi/10.1145/1571941.1572114); [bge-reranker-v2-m3](https://huggingface.co/BAAI/bge-reranker-v2-m3).)
- **Recall: höchster** — echte Bedeutungs-/Cross-Lingual-Matches, die die Lexik-Legs prinzipiell nicht können; RRF vereint lexikalisch+dicht, Cross-Encoder+Gate liefern Top-1-Präzision. **Aufwand: L** (1–3 Wochen für Tier-2/dicht; +mehr für Fusion/Rerank). **Infra:** Embedding-Host (GPU für Build, CPU ok für Query) + FAISS/Qdrant (~1 GB RAM/100k) + optional Cross-Encoder-Host. **Latenz:** Encode (ms GPU / ~10er ms CPU) + `<10 ms` ANN + Rerank ~10er–100er ms. **$:** moderat; Corpus-Refresh bei Ontologie-Versionssprung (offline; Index zwischen Rebuilds veraltet, Query-Seite immer live). **Abhängigkeiten:** Tier-0-Routing wählt zu embeddende Ontologien.

---

## D. Empfohlene Sequenz

**Prototyp-Reihenfolge (billigstes zuerst de-risken):**
1. **Tier 0 (Stunden).** `also_search_properties=true`, `language=<Quellsprache>`, Pagination/`pagesize↑`. Null Infra, greift sofort Grund 3+4.
2. **Tier 1 (Tage).** Annotator-Leg (Term + Definition, `longest_only`, `expand_mappings`). In-API pseudo-semantischer Recall.
3. **Tier 2 (Tage).** Query-Expansion — **DE→EN + LLM-Synonyme** zuerst (größter, billigster Gewinn für ein deutsches Toolkit), `derive_*`-Helfer auf Hauptpfad heben, optional UMLS-CUI-Atome. Schließt Sprach- + Paraphrasen-Lücke ohne Vektor-DB.
4. **Tier 3 (Wochen) — nur falls die Lücke bleibt.** Embedding-Index über die gerouteten Ontologien; RRF + Cross-Encoder-Rerank + Typ-Gate, wenn Top-1-Präzision zählt.

**Was messen** (Gold-Set aus Term→korrektes-Konzept-Paaren, idealerweise deutsche Terme):
- **Recall@k** (erscheint das korrekte Konzept in `raw_candidate_scores` *vor* der Kürzung?) — isoliert *Retrieval* von Adjudikation, misst Abschnitt A direkt.
- **Recall@k nach Fehlergrund** (Paraphrase / cross-lingual / definition-only / tief-gerankt) — zeigt, welches Tier welchen Grund behebt.
- **Precision@1 nach Adjudikation** + **Pool-Rauschen** (Poolgröße vs. LLM-Kosten).
- **Latenz + API-Call-Zahl pro Term** (gegen die 15-req/s-Decke).

**Entscheidungsbaum:**
```
Tier 0 + Tier 1 auf dem Gold-Set laufen lassen.
├─ Recall@k schon akzeptabel (korrektes Konzept fast immer im Pool)?
│    → STOP. Tier 3 nicht nötig. Optional nur Tier-2-Übersetzung, falls
│      deutsche Terme die Rest-Fehler sind.
└─ Noch korrekte Konzepte fehlend?
     ├─ Fehler v.a. cross-lingual oder Abk./Paraphrase?
     │    → Tier 2 (Übersetzung + LLM/UMLS-Synonym-Expansion). Neu messen.
     │      Die meisten Rest-Misses schließen hier ohne Vektor-DB.
     └─ Echte Paraphrasen OHNE geteiltes Token UND ohne kuratiertes Synonym/Mapping,
        oder Tier-2-Recall noch zu kurz?
             → Tier 3 (Embedding-ANN) bauen, dann RRF + Cross-Encoder-Rerank + Typ-Gate
               nur falls Top-1-Präzision (nicht nur Recall) das Restproblem ist.
```

**Leitprinzip (aus B):** Tiers 0–2 schöpfen BioPortals *pseudo-semantische* Fläche billig aus und lösen die Mehrheit heutiger Misses (Token-/Feld-/Scope-/Sprach-Fehler, keine tiefe Paraphrase). Nur echte token-disjunkte Paraphrasen *erfordern* Tier-3-Embeddings — deshalb diesen Rest erst **messen**, bevor die L-Aufwand-Vektorschicht investiert wird.

---

**Referenzierte Dateien/Nähte:** [agent_bioportal_service.py](agentic_reconciliation/agent_bioportal_service.py) (`search_bioportal_candidates`:354, Params 361-371, `AgentCandidate`-Mapping 384-401, `find_term_in_ontology*` 124-290 = einziger `require_exact_match`-Ort) · [agent_orchestrator_workflows.py](agentic_reconciliation/agent_orchestrator_workflows.py) (`raw_candidate_scores`-Init ~3016, Append-Idiom 3112, Dedupe/Rank ~3353-3364, Survival-Filter ~3374-3380, Einfügefenster ~3127-3242) · [agent_rescue_adjudication.py](agentic_reconciliation/agent_rescue_adjudication.py) (`derive_abbreviation_expansions`:207, `derive_definition_queries`:254 — rescue-only, für Tier 2 wiederverwendbar) · [agent_ontology_routing.py:2171](agentic_reconciliation/agent_ontology_routing.py:2171) (5-Ontologien-Cap). BioPortal-Solr-Interna: ncbo/ontologies_api `helpers/search_helper.rb` + `test/solr/.../schema.xml`; API-Doku data.bioontology.org/documentation.
