## Purpose:
This document defines the HTTPS API endpoints and their signatures that return JSON data between the Backend (a flask process linked to the humaniki database) and your app or our Frontend dashboard javascript-react app.
For a visual layout see https://miro.com/app/board/o9J_kmRfpZ0=/

## Metrics API
## Required base parameter
the base of all URL requests begin 
`humanikidata.org/api/{version}/{interest}/{facet}/`
N.b. `humanikidata.org` points to `humaniki.wmcloud.org` for now, so if you want a small speed improvment, use the resolving domain.

1. Version
   1. `v1` (as of 2021)
2. Interest
   1. `gender`
   2. `race`  (planned, not yet supported)
   3. `disability`  (planned, not yet supported)
   4. `sexuality`  (planned, not yet supported)
   5. `spoken language`  (planned, not yet supported)
   6. `class`  (planned, not yet supported)
   7. `available_snapshots` - metadata about humaniki's historical dataprocessing
3. Facet
   1. `gap` - counts of humans by wikidata property values 
   2. `evolution` - longitudinal gap data - (planned, not yet supported)
   3. `list` - lists of humans with property values - (planned, not yet supported)



### Facet = "gap"
* Syntax: `/v1/gender/gap/{snapshot}/{population}/properties{?query-string)`
* Examples: `https://humaniki.wmcloud.org/api/v1/gender/gap/latest/gte_one_sitelink/properties?date_of_birth=1900~2000&label_lang=en`
* params
   * Snapshot
      * "latest"
      * Or YYYY-MM-DD (from available_snapshots interest)
   * Population-definition
      * "all-wikidata" any wikidata item
      * "gte-one-sitelink" any wikidata item with greater than or equal to one sitelink
      * "sitelink-multiplicity" -  not yet supported
   * Properties query string
      * Some & joined combination of 
      * Country (QID)
         * "all" or
         * country-qid without Q
      * Year of birth (YYYY)
         * Single YYYY year or
         * tilde joined list of STARTYYYY-ENDYYYY
            * half-open ranges allowed
      * Occupation (QID)
         * "all" or
         * occupation qid without Q
      * Project (wikicode)
         * 'all'
         * Or language code, like 'enwiki', or 'commonswiki' or 'frwikisource'
      * label_lang
        * two-letter iso3066 code to get label translations from wikidata.
        * 'en' only for now


#### Example Return Values
   * Gender-by-language
      * `api/v1/gender/gap/latest/gte_one_sitelink/properties?project=all`
      * Note population='all-wikidata' doesn't make sense for things where projects are specified, so the api will correct this and tell you in a response.
      * Return value
```
"meta":
    {"aggregation_properties":["PROJECT"],
     "bias": "gender",
     "bias_labels":
        {"6581072":"female",
         "6581097": "male",
         "7130936": "pangender",
        "859614": "bigender",
         "93954933": "demiboy",
         "96000630":"X-gender"},
     "bias_property": 21, // corresponds to P21
     "coverage": 0.72, // percentage of humans having these properties compared to all humans with this population
     "label_lang": "en",
     "population": "GTE_ONE_SITELINK",
     "population_corrected": false,
     "snapshot": "2020-11-09"},
"metrics":[
  {"item": {"project": "abwiki"},
     "item_label": {"project": "Abkhaz Wikipedia"},
     "order": 0,
     "values": 
      {"6581072": 11, // refer to meta.bias_labels to resolve this gender string
      "6581097":144}},
  {"item": {"project":"acewiki"},
     "item_label": {"project": "Acehnese Wikipedia"},
     "order": 1,
     "values":
        "6581072": 69,
        "6581097": 361}}
  // ... many more
]
```

### Facet = evolution - coming soon.
* Not-implemented in phase 1
* Properties
* Snapshot-range


### Facet = list - coming soon.
* Not-implemented in phase 1
* Properties
* Snapshot-range


### Facet Snapshots - coming soon.
{version}/available_snapshots/
* Snapshot-dates in reverse chronological order.
