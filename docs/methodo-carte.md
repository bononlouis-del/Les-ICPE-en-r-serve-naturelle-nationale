# Méthodologie — Carte interactive des ICPE en Gironde

Ce document décrit la chaîne de production des données affichées par la
carte interactive, et la méthodologie de l'audit des coordonnées mené
avant l'enquête.

---

## 1. Données sources

### 1.1 Export officiel Géorisques (source canonique)

La source principale est l'**export bulk CSV** publié par l'API
Géorisques V1 du Ministère de la Transition Écologique :

- **Point d'accès** : `GET /api/v1/csv/installations_classees?departement=33`
- **Date de téléchargement** : 8 avril 2026
- **Format** : archive ZIP contenant 5 fichiers CSV (encodage ISO-8859-1,
  séparateur `;`), convertis en UTF-8 à l'extraction
- **Contenu** :
  - `InstallationClassee.csv` — 2 890 installations (table principale)
  - `inspection.csv` — historique d'inspection
  - `rubriqueIC.csv` — rubriques ICPE par installation
  - `metadataFichierInspection.csv` — métadonnées des rapports publiables
  - `metadataFichierHorsInspection.csv` — métadonnées des documents non publiables

Le script `scripts/fetch_georisques.py` automatise le téléchargement,
l'archivage horodaté dans `données-georisques/raw/`, la conversion en
UTF-8, et la comparaison avec la source historique.

### 1.2 Snapshot historique data.gouv.fr (référence secondaire)

Un export CSV plus ancien (février 2025) est conservé dans
`carte/liste-icpe-gironde.csv` à titre de comparaison temporelle. Il
liste 2 888 installations.

La comparaison entre les deux sources montre :

| Source | Installations |
|---|---:|
| Bulk Géorisques (avril 2026) | 2 890 |
| CSV historique (février 2025) | 2 888 |
| En commun | 2 886 |
| Ajoutées depuis février 2025 | 4 |
| Retirées depuis février 2025 | 2 |

Les 4 ajouts correspondent à des créations ou réinscriptions récentes.
Les 2 retraits sont une radiation administrative (SEMOCTOM) et une
entrée fantôme filtrée de l'export public (PESA). La différence est
purement temporelle et attendue.

### 1.3 Réserves naturelles (IGN Géoplateforme)

Les polygones des réserves naturelles sont téléchargés via le service
WFS de la Géoplateforme de l'IGN :

- **RNN** (Réserves Naturelles Nationales) : couche `patrinat_rnn:rnn`
  — 9 réserves en Gironde
- **RNR** (Réserves Naturelles Régionales) : couche `patrinat_rnr:rnr`
  — 0 réserve en Gironde

L'emprise de filtrage couvre la Gironde : longitude −1,3° à 0,3°,
latitude 44,2° à 45,6°.

### 1.4 Communes et EPCI (geo.api.gouv.fr)

Les contours communaux et les rattachements EPCI proviennent de l'API
géographique d'Etalab (géo-découpage administratif) :

- Communes : `https://geo.api.gouv.fr/departements/33/communes?fields=nom,code,codeEpci`
- EPCI : `https://geo.api.gouv.fr/epcis?fields=nom,code`

Le résultat est mis en cache localement (48 Ko, JSON compact) pour
permettre les exécutions hors-ligne.

### 1.5 Rapports d'inspection (Géorisques)

Les rapports d'inspection publiables sont téléchargés depuis l'endpoint
Géorisques dédié par le script `scripts/telecharger_rapports_inspection.py`.
Le téléchargement est idempotent (les fichiers déjà présents ne sont pas
retéléchargés) et les erreurs HTTP 404 sont mémorisées pour ne pas
relancer de requêtes inutiles aux lancements suivants.

---

## 2. Enrichissement des données

Le script `scripts/enrichir_libelles.py` transforme l'export brut en un
CSV exploitable par la carte. Il produit deux fichiers :

1. **`données-georisques/InstallationClassee_enrichi.csv`** — le bulk
   enrichi (2 890 lignes, séparateur `;`)
2. **`carte/data/liste-icpe-gironde_enrichi.csv`** — la projection vers
   le format de la carte (2 890 lignes, séparateur `,`)

### 2.1 Architecture « Scope Y' » (bulk-canonique)

La carte est produite **à partir du bulk officiel** (2 890 lignes),
avec un left-join sur le snapshot historique uniquement pour récupérer
deux champs absents du bulk : la date de création (`cdate`) et
l'identifiant historique (`gid`). Les 4 installations présentes
seulement dans le bulk reçoivent des valeurs vides pour ces champs.

La jointure se fait par la clé `codeAiot` (bulk) ↔ `ident` (historique),
normalisée en supprimant les zéros à gauche.

### 2.2 Normalisation catégorielle

Les valeurs textuelles du bulk sont normalisées pour correspondre aux
catégories attendues par les filtres de la carte :

**Régime ICPE :**

| Valeur Géorisques | Valeur normalisée |
|---|---|
| Autorisation | AUTORISATION |
| Enregistrement | ENREGISTREMENT |
| Autres régimes | AUTRE |
| Non ICPE | NON_ICPE |

**Statut Seveso :**

| Valeur Géorisques | Valeur normalisée |
|---|---|
| Non Seveso | NON_SEVESO |
| Seveso seuil bas | SEUIL_BAS |
| Seveso seuil haut | SEUIL_HAUT |
| *(vide)* | *(vide)* |

**Colonnes booléennes** (`bovins`, `porcs`, `volailles`, `carriere`,
`eolienne`, `industrie`, `prioriteNationale`, `ied`) : les valeurs
`"true"`/`"false"` du bulk sont converties en `"TRUE"`/`"FALSE"`.

Si une valeur catégorielle inconnue apparaît dans un futur export (par
exemple un nouveau régime ICPE), le script émet un avertissement et
applique un fallback (`AUTRE` pour le régime, `NON_SEVESO` pour Seveso)
au lieu de laisser la valeur brute corrompre silencieusement le CSV.

### 2.3 Désambiguïsation des libellés

L'export Géorisques contient des libellés non uniques (une même raison
sociale peut désigner plusieurs établissements). Le script produit un
**libellé complet unique** (`libelle_complet`) par installation en
deux passes :

1. **Classification** : les libellés contenant un séparateur ` - ` ou
   ` – ` sont décomposés en `structure` (avant le séparateur) et
   `etablissement` (après). Les libellés sans séparateur sont marqués
   comme potentiellement ambigus si le même texte apparaît plus d'une
   fois.

2. **Désambiguïsation progressive** : pour chaque groupe de libellés
   identiques, la commune puis l'adresse sont ajoutées au nom de
   l'établissement jusqu'à ce que chaque ligne soit unique. En dernier
   recours, un suffixe numérique `(#1, #2, …)` est ajouté dans l'ordre
   du code AIOT.

### 2.4 Enrichissement communal et EPCI

Pour chaque installation, le code INSEE est utilisé pour récupérer :

- le nom normalisé de la commune (source : IGN / Admin Express via
  geo.api.gouv.fr)
- le code SIREN et le nom de l'EPCI de rattachement

Ce lookup est mis en cache pour permettre les exécutions sans connexion.

### 2.5 Synthèse des coordonnées géographiques

Les colonnes `longitude` et `latitude` du bulk sont combinées en deux
colonnes GeoJSON utilisées par la carte :

- `Geo Point` : `"lat, lon"` (ordre latitude-longitude pour compatibilité
  avec les conventions cartographiques)
- `Geo Shape` : objet GeoJSON Point `{"type": "Point", "coordinates": [lon, lat]}`

---

## 3. Audit des coordonnées

### 3.1 Objectif

Pour chaque installation, Géorisques fournit à la fois une **adresse
postale** et des **coordonnées géographiques** (latitude/longitude).
Quand les deux ne concordent pas, la position du site sur la carte peut
être trompeuse — en particulier quand l'écart fait basculer la réponse
à la question : « ce site est-il dans une réserve naturelle ? ».

L'audit vérifie systématiquement la cohérence entre adresse et
coordonnées pour les 2 890 installations, en croisant cinq signaux
complémentaires.

### 3.2 Les cinq passes de signaux

Le script `scripts/audit_coordinates.py` exécute cinq passes
séquentielles. Chaque passe enrichit la ligne avec de nouvelles colonnes
de signaux, que la passe de classification finale agrège.

#### Passe 1 — Sentinelles (hors-ligne)

Détecte les anomalies structurelles sans appel réseau :

| Signal | Condition | Signification |
|---|---|---|
| `null_island` | Les deux coordonnées sont à (0, 0) ou manquantes | Coordonnées invalides ou absentes |
| `outside_gironde` | Le point stocké est hors du contour départemental | Site probablement mal géolocalisé dans Géorisques |
| `commune_centroid` | Le point stocké est à moins de 50 m du centroïde de sa commune | Signature typique d'un géocodeur qui n'a pas trouvé l'adresse et a placé le point au centre de la commune |
| `duplicate_coords` | 3 sites ou plus partagent les mêmes coordonnées arrondies à 5 décimales | Coordonnées copiées ou attribution par défaut |

#### Passe 2 — Point-in-polygon communal

Teste si le point stocké est géométriquement à l'intérieur du polygone
de sa commune déclarée, en utilisant les contours communaux de l'IGN :

| Résultat | Signification |
|---|---|
| `true` | Le point est dans la bonne commune |
| `false` | Le point est en Gironde mais dans une autre commune que celle déclarée |
| `null` | Le polygone de la commune n'est pas disponible (ne peut pas vérifier) |

#### Passe 3 — Géocodage direct (forward geocoding)

**Principe** : à partir de l'adresse postale de chaque installation,
interroger un géocodeur pour obtenir des coordonnées indépendantes, puis
mesurer la distance par rapport aux coordonnées stockées dans
Géorisques. Si la distance est grande, c'est un signal que les
coordonnées stockées sont probablement fausses.

Le géocodage utilise une **cascade à trois couches** pour maximiser le
taux de résolution :

**Couche 1 — BAN** (Base Adresse Nationale, api-adresse.data.gouv.fr) :

L'adresse est soumise en trois stratégies successives. Chaque stratégie
est une construction différente de la chaîne envoyée au géocodeur :

| Stratégie | Chaîne envoyée | Logique |
|---|---|---|
| adresse1 seule | Contenu du champ `adresse1` | La plupart des sites ont leur rue dans ce champ |
| adresse2 seule | Contenu du champ `adresse2` | Certains sites mettent le nom du lieu-dit en adresse1 et la rue en adresse2 |
| combinée | `adresse1` + `adresse2` concaténées | Dernier recours si les deux champs seuls échouent |

La première stratégie qui produit un résultat avec un score ≥ 0,4
l'emporte. Chaque stratégie est mise en cache séparément : une
ré-exécution du script ne refait que les appels réseau nécessaires.

**Résultat** : sur les 2 890 sites, la BAN a résolu **2 175 adresses**
(75,3 %).

**Couche 2 — OpenCage** (opencagedata.com) :

Pour les 715 adresses que la BAN n'a pas pu résoudre, le géocodeur
OpenCage est interrogé. OpenCage agrège les données d'OpenStreetMap,
GeoNames et d'autres sources.

- Limite : 2 500 requêtes/jour, 1 requête/seconde (palier gratuit)
- Clé API requise (variable d'environnement `OPENCAGE_API_KEY`)
- Résultats mis en cache localement pour ne pas re-consommer le quota

**Résultat** : OpenCage a résolu **714 adresses** supplémentaires (24,7 %).

**Couche 3 — Nominatim** (OpenStreetMap) :

Dernière couche, utilisée uniquement pour les résultats faibles
d'OpenCage (résolution au niveau commune/localité seulement). Les
correspondances OpenCage de type `locality` ou `municipality` sont
re-soumises à Nominatim pour tenter d'obtenir une résolution plus
précise (rue ou numéro de rue).

- Limite : 1 requête/seconde, identification par User-Agent obligatoire
- Résultats mis en cache localement

**Résultat** : Nominatim a amélioré **1 résolution** sur les cas resoumis.

**Bilan du géocodage forward** : 100 % de couverture (2 890 / 2 890),
répartis par précision :

| Précision du résultat | Nombre | Part |
|---|---:|---:|
| Numéro de rue (`housenumber`) | 842 | 29,1 % |
| Rue (`street`) | 1 531 | 53,0 % |
| Localité (`locality`) | 332 | 11,5 % |
| Commune (`municipality`) | 185 | 6,4 % |

#### Passe 4 — Géocodage inverse (reverse geocoding)

**Principe** : à partir des coordonnées stockées dans Géorisques,
interroger la BAN pour obtenir l'adresse correspondante. L'objectif est
de vérifier la cohérence dans l'autre sens : « quelle adresse se trouve
au point enregistré ? ».

Ce signal est utilisé de deux façons :

1. **Détection de commune erronée** : si le code INSEE retourné par le
   géocodage inverse ne correspond pas au code INSEE déclaré par le
   site, c'est un signal fort que les coordonnées stockées sont dans la
   mauvaise commune.

2. **Confirmation communale** : quand le géocodage direct a échoué (pas
   d'adresse résolue), si le géocodage inverse confirme que le point
   stocké est dans la bonne commune, c'est un signal faible mais positif
   que les coordonnées sont au moins dans le bon secteur.

**Taux de couverture** : la BAN inverse a retourné un résultat pour
**2 544 sites** (88 %). Les 346 échecs correspondent à des points dans
des zones non couvertes par la BAN (champs, zones industrielles non
cartographiées, zones rurales à faible couverture).

#### Passe 5 — Appartenance aux réserves naturelles

Pour chaque site, le script teste si le point stocké ET le point géocodé
sont à l'intérieur d'une réserve naturelle (test point-in-polygon contre
les 9 polygones RNN).

Quatre signaux sont produits :

| Signal | Signification |
|---|---|
| `stored_in_reserve` | Nom de la réserve contenant le point stocké, ou `"none"` |
| `geocoded_in_reserve` | Nom de la réserve contenant le point géocodé, ou `"none"` |
| `reserve_ambiguous` | `true` si les deux points ne sont pas d'accord sur l'appartenance |
| `reserve_boundary_proximity` | `true` si l'un des deux points est à moins de 200 m d'une limite de réserve |

Le cas le plus critique est `reserve_ambiguous` : le point stocké dit
que le site est dans une réserve mais le géocodage dit le contraire (ou
inversement). Ces cas nécessitent une vérification humaine prioritaire.

### 3.3 Classification

Après les cinq passes, chaque site reçoit une **classe d'audit**
attribuée par une échelle de priorité (la première condition vérifiée
l'emporte) :

| Classe | Condition | Signification |
|---|---|---|
| `null_island` | Coordonnées à (0, 0) ou manquantes | Données structurellement invalides |
| `outside_gironde` | Le point est hors du département | Géolocalisation manifestement fausse |
| `wrong_commune` | Le point n'est pas dans la commune déclarée (PIP ou reverse) | Erreur de commune |
| `address_unresolvable_isolated` | Géocodage direct échoué ET le reverse n'a pas confirmé la commune | Aucun signal exploitable |
| `address_unresolvable_commune_ok` | Géocodage direct échoué MAIS le reverse confirme la bonne commune | Adresse introuvable mais coordonnées probablement dans le bon secteur |
| `address_imprecise` | Géocodage résolu au niveau commune ou localité (pas rue ni numéro) | Résolution trop grossière pour conclure |
| `very_severe` | Distance stocké ↔ géocodé ≥ 2 000 m | Écart très important |
| `severe` | Distance ≥ 500 m | Écart important |
| `suspicious` | Distance ≥ 100 m | Écart à vérifier |
| `minor` | Distance ≥ 25 m | Écart mineur |
| `ok` | Distance < 25 m | Coordonnées cohérentes |

### 3.4 Résultats de l'audit (8 avril 2026)

**Répartition par classe :**

| Classe | Effectif |
|---|---:|
| ok | 169 |
| minor | 323 |
| suspicious | 530 |
| severe | 584 |
| very_severe | 377 |
| outside_gironde | 8 |
| wrong_commune | 59 |
| address_unresolvable_commune_ok | 315 |
| address_unresolvable_isolated | 57 |
| address_imprecise | 468 |
| null_island | 0 |
| **Total** | **2 890** |

**Interprétation** : seuls 169 sites (5,8 %) ont des coordonnées
parfaitement cohérentes avec leur adresse. Pour la majorité des sites,
un écart mesurable existe entre l'adresse postale et la position
géographique enregistrée dans Géorisques.

Cela ne signifie pas nécessairement que les coordonnées sont fausses :
un écart de quelques centaines de mètres peut refléter le fait que le
géocodeur a trouvé la rue mais pas le numéro précis, ou que
l'installation est effectivement éloignée du front de rue (cas fréquent
pour les carrières, les éoliennes, et les installations en zone
industrielle).

### 3.5 Groupes de revue

Les sites sont répartis en trois groupes pour la revue collaborative :

| Groupe | Critère | Effectif | Priorité |
|---|---|---:|---|
| **Réserves** | L'appartenance à une réserve diffère entre le point stocké et le point géocodé, ou un point est à moins de 200 m d'une limite | 1 | Critique |
| **Grands écarts** | Classe `very_severe`, `severe`, `outside_gironde`, `wrong_commune`, `address_imprecise`, ou `address_unresolvable_isolated` | 1 552 | Haute |
| **Petits écarts** | Classe `suspicious`, `minor`, ou `address_unresolvable_commune_ok` | 1 168 | Basse |
| *(non flagué)* | Classe `ok`, pas de signal réserve | 169 | — |

---

## 4. Outil de revue collaborative

### 4.1 Principe

L'outil de revue (`/audit/`) permet à plusieurs enquêteurs de vérifier
les écarts en parallèle sans conflit d'écriture. Les sites à vérifier
sont découpés en **buckets** de 25 sites chacun. Chaque enquêteur prend
un bucket, examine chaque site, et enregistre son verdict.

### 4.2 Ce que voit l'enquêteur

Pour chaque site, l'outil affiche :

1. **Identité** : nom complet, SIRET, régime ICPE, statut Seveso, lien
   vers la fiche Géorisques
2. **Coordonnées enregistrées** : l'adresse et les coordonnées telles
   qu'elles figurent dans Géorisques
3. **Adresse géocodée** : le résultat du géocodage direct (BAN /
   OpenCage / Nominatim), avec le niveau de précision (numéro, rue,
   localité…) et la distance par rapport aux coordonnées enregistrées
4. **Adresse au point enregistré (reverse)** : ce que la BAN retourne
   quand on lui soumet les coordonnées stockées — « quelle adresse se
   trouve à cet endroit ? ». Si le résultat indique une commune
   différente de celle déclarée, c'est un signal fort d'erreur.
5. **Signaux d'audit** : la classe attribuée par le pipeline, et les
   éventuels signaux de réserve naturelle

Une **mini-carte** affiche les deux points (coordonnées stockées en
rouge, coordonnées géocodées en bleu) avec un trait matérialisant la
distance entre les deux. Deux fonds de carte sont disponibles : plan
(CartoDB Voyager) et orthophotographie (IGN).

### 4.3 Scénarios de revue

#### Cas 1 — Géocodage direct et inverse ont fonctionné, écart significatif

C'est le cas le plus fréquent. L'enquêteur voit les deux points sur la
carte et la distance entre eux. L'adresse inverse lui dit « ce qui se
trouve réellement à l'emplacement enregistré ». Il décide quel point
est correct.

#### Cas 2 — Géocodage direct trop imprécis (468 sites `address_imprecise`)

Le géocodeur n'a résolu l'adresse qu'au niveau de la commune ou du
quartier, pas de la rue. Le point géocodé est donc le centroïde de la
commune — la distance affichée n'est pas significative.

L'enquêteur doit s'appuyer sur l'adresse inverse (si disponible) et
sur la fiche Géorisques pour évaluer si les coordonnées stockées sont
vraisemblables.

#### Cas 3 — Géocodage direct échoué, reverse confirme la commune (315 sites)

Aucun géocodeur (BAN, OpenCage, Nominatim) n'a pu résoudre l'adresse.
En revanche, le géocodage inverse confirme que les coordonnées stockées
sont dans la bonne commune. C'est un signal faible mais positif : les
coordonnées sont probablement dans le bon secteur.

L'enquêteur voit « *(non géocodé)* » dans la section géocodage, et
l'adresse inverse dans la section correspondante. Il peut généralement
faire confiance aux coordonnées stockées pour ces sites.

#### Cas 4 — Géocodage direct ET inverse échoués (57 sites)

Le pipeline n'a trouvé aucun signal automatique. Ni le géocodage direct
(adresse → point) ni le géocodage inverse (point → adresse) n'ont
produit de résultat exploitable.

L'enquêteur voit « *(non géocodé)* » et « *(non disponible)* ». Il doit
investiguer manuellement : consulter la fiche Géorisques, vérifier sur
un moteur de recherche cartographique, ou marquer le site pour une
visite terrain.

#### Cas 5 — Commune erronée (59 sites `wrong_commune`)

Le test point-in-polygon ou le géocodage inverse indique que les
coordonnées stockées pointent vers une commune différente de celle
déclarée par le site. L'enquêteur doit déterminer si c'est la commune
déclarée qui est fausse, les coordonnées qui sont fausses, ou un
problème de limites communales (site en bordure).

### 4.4 Verdicts disponibles

Pour chaque site, l'enquêteur choisit parmi quatre verdicts :

| Verdict | Action | Quand l'utiliser |
|---|---|---|
| **Garder les coordonnées enregistrées** | Aucune correction | Les coordonnées Géorisques sont correctes ; l'écart vient du géocodeur |
| **Utiliser l'adresse géocodée** | Remplacer par les coordonnées BAN/OpenCage | L'adresse géocodée pointe au bon endroit, les coordonnées Géorisques sont fausses |
| **Placer manuellement** | L'enquêteur place un point sur la carte (clic ou mode clavier) | Ni les coordonnées stockées ni le géocodage ne sont satisfaisants |
| **Visite terrain** | Reporter la décision | Le cas nécessite une vérification physique |

L'enquêteur peut aussi cocher « Pertinent pour l'enquête » pour marquer
les sites qui présentent un intérêt journalistique particulier (par
exemple un site Seveso seuil haut dont les coordonnées sont fausses et
le placent dans une réserve naturelle).

### 4.5 Collaboration

Les verdicts sont exportés sous forme de fichiers JSON par bucket et
commités dans le dépôt GitHub. L'outil de revue découvre
automatiquement les fichiers commités par les autres enquêteurs via
l'API GitHub Contents et met à jour l'affichage de progression. Un
système de backoff adaptatif évite d'épuiser le quota de l'API GitHub
(60 requêtes/heure sans authentification).

---

## 5. La carte interactive

### 5.1 Données affichées

La carte (`/carte/`) consomme le CSV enrichi
`carte/data/liste-icpe-gironde_enrichi.csv` (2 890 sites) et affiche
chaque site comme un marqueur ponctuel, regroupé en clusters aux
niveaux de zoom faibles.

### 5.2 Filtres disponibles

| Dimension | Valeurs |
|---|---|
| Régime ICPE | AUTORISATION, ENREGISTREMENT, NON_ICPE, AUTRE |
| Statut Seveso | SEUIL_HAUT, SEUIL_BAS, NON_SEVESO |
| Priorité nationale | oui / non |
| Directive IED | oui / non |
| Secteur d'activité | industrie, carrière, autre |
| Commune / EPCI | recherche textuelle |
| Période de création | filtre mensuel via `cdate` |

### 5.3 Couches cartographiques

- **Fond plan** : CartoDB Voyager (tuiles raster)
- **Orthophotographie** : IGN HR.ORTHOIMAGERY.ORTHOPHOTOS (WMTS)
- **Contour départemental** : GeoJSON Gironde
- **Communes** : polygones GeoJSON avec opacité réglable
- **EPCI** : contours calculés depuis les communes (script
  `carte/scripts/build_epci_outlines.py`)
- **Réserves naturelles** : polygones GeoJSON (RNN + RNR)

---

## 6. Reproductibilité

L'ensemble du pipeline est reproductible et idempotent :

1. `python3 scripts/fetch_georisques.py` — télécharge l'export bulk,
   l'archive dans `données-georisques/raw/`, compare avec le snapshot
   historique
2. `python3 scripts/enrichir_libelles.py` — enrichit le bulk et produit
   le CSV de la carte
3. `OPENCAGE_API_KEY=… uv run scripts/audit_coordinates.py` — exécute
   les 5 passes d'audit. Les résultats des appels réseau (BAN, OpenCage,
   Nominatim) sont mis en cache : une ré-exécution ne refait que les
   appels nécessaires.
4. `python3 scripts/telecharger_rapports_inspection.py` — télécharge
   les rapports d'inspection publiables

Chaque étape vérifie ses prérequis (présence du fichier d'entrée,
colonnes attendues) et écrit ses sorties de façon atomique (écriture
dans un fichier temporaire puis remplacement, via `os.replace`) pour
éviter les états intermédiaires en cas d'interruption.
