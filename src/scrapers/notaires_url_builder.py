"""
Générateur d'URLs pour immobilier.notaires.fr/fr/prix-immobilier

Construit des URLs de prix au m² en partant de la localisation la plus précise
et en remontant vers la plus large.
"""

import requests
from urllib.parse import urlencode

# ---------------------------------------------------------------------------
# BASE URL
# ---------------------------------------------------------------------------

NOTAIRES_BASE_URL = "https://www.immobilier.notaires.fr/fr/prix-immobilier"

# ---------------------------------------------------------------------------
# PARAMÈTRES CONFIRMÉS PAR OBSERVATION DES URLs INDEXÉES
# ---------------------------------------------------------------------------

# typeLocalisation : niveau géographique
# Valeurs confirmées (du plus précis au plus large) :
#   GRAND_QUARTIER  -> codeInsee = 7 chiffres (ex: 9205104, 7510501)
#   ARRONDISSEMENT  -> codeInsee = 5 chiffres (ex: 75105, 13202)
#   COMMUNE         -> codeInsee = 5 chiffres (ex: 75056, 92051)
#   DEPARTEMENT     -> codeInsee = 2 ou 3 chiffres (ex: 75, 92, 974)
#   REGION          -> codeInsee = 2 chiffres (ex: 11 = Île-de-France)
#   FRANCE          -> pas de codeInsee

# neuf : état du bien
#   A     -> Ancien (confirmé massivement dans les URLs indexées)
#   false -> Vu une seule fois (Paris 13e), possiblement "tous"
#   NOTE: pas de valeur "N" ou "O" confirmée. Probable : A=ancien, N=neuf, absent=tous

# typeBien : type de bien
#   APP -> Appartement (confirmé dans l'URL exemple)
#   MAI -> Maison (confirmé par documentation générale)
#   TER -> Terrain (probable, non confirmé dans URLs indexées)

# piece : nombre de pièces
#   Valeurs observées dans l'URL exemple : 4
#   ATTENTION : "6 pièces et plus" = piece=7 (selon l'utilisateur)
#   Hypothèse : 1, 2, 3, 4, 5, 6 (=5 pièces), 7 (=6 pièces et plus)
#   -> À CONFIRMER via l'interface

# surfaceMin : surface minimum en m²
#   Valeur observée : 7 (dans l'URL exemple)
#   Ce sont probablement des tranches prédéfinies, pas un champ libre
#   -> À CONFIRMER via l'interface

# stationnement : présence de parking
#   Valeur observée : 1 (dans l'URL exemple)
#   Hypothèse : 1 = avec stationnement, 0 ou absent = sans filtre
#   -> À CONFIRMER via l'interface

# ---------------------------------------------------------------------------
# MAPPING DES FILTRES PAR TYPE DE BIEN
# ---------------------------------------------------------------------------

NOTAIRES_FILTERS = {
    "APP": {
        "label": "Appartement",
        "pieces": {
            1: "1 pièce",
            2: "2 pièces",
            3: "3 pièces",
            4: "4 pièces",
            5: "5 pièces",
            # 6: "6 pièces",       # À confirmer
            7: "6 pièces et plus",  # Confirmé par observation utilisateur
        },
        "surfaces": [7],  # Seule valeur observée — probablement des tranches
        # Hypothèse pour APP : [7, 15, 25, 35, 50, 70, 90, 110, 130, 150]
        "stationnement": {
            1: "Avec stationnement",
            # 0: "Sans filtre",  # À confirmer
        },
    },
    "MAI": {
        "label": "Maison",
        "pieces": {
            1: "1 pièce",
            2: "2 pièces",
            3: "3 pièces",
            4: "4 pièces",
            5: "5 pièces",
            7: "6 pièces et plus",
        },
        "surfaces": [],  # Non observé — probablement différent de APP
        # Hypothèse pour MAI : tranches plus grandes (50, 70, 90, 110, 130, 150, 200)
        "stationnement": {
            1: "Avec stationnement",
        },
    },
    "TER": {
        "label": "Terrain",
        "pieces": {},  # Non applicable
        "surfaces": [],  # Probablement en m² de terrain
        "stationnement": {},  # Non applicable
    },
}

# Valeurs de neuf
NEUF_VALUES = {
    "A": "Ancien",
    # "N": "Neuf",          # Hypothèse non confirmée
    # None: "Tous (défaut)", # Hypothèse
}

# Codes département pour Paris/Lyon/Marseille (arrondissements)
VILLES_ARRONDISSEMENTS = {
    "75": {"name": "Paris", "start": 75101, "end": 75120},
    "69123": {"name": "Lyon", "start": 69381, "end": 69389},
    "13055": {"name": "Marseille", "start": 13201, "end": 13216},
}

# ---------------------------------------------------------------------------
# FONCTIONS
# ---------------------------------------------------------------------------


def build_location_candidates(citycode: str, code_iris: str | None = None, postcode: str | None = None) -> list[dict]:
    """
    Génère les candidats de localisation du plus précis au plus large.

    Args:
        citycode: Code INSEE de la commune (5 chiffres, ex: "75105", "92051")
        code_iris: Code IRIS complet (9 chiffres, ex: "751050101") ou None
        postcode: Code postal (ex: "75005") — non utilisé dans l'URL mais utile
                  pour déterminer le département

    Returns:
        Liste de dicts [{"typeLocalisation": ..., "codeInsee": ...}, ...]
        ordonnée du plus précis au plus large.
    """
    candidates = []
    dept = citycode[:2] if len(citycode) == 5 else citycode[:3]

    # 1. GRAND_QUARTIER si IRIS disponible et pas un IRIS "0000"
    if code_iris and len(code_iris) >= 7:
        iris_suffix = code_iris[5:9] if len(code_iris) >= 9 else code_iris[5:]
        if iris_suffix != "0000":
            grand_quartier_code = code_iris[:7]
            candidates.append({
                "typeLocalisation": "GRAND_QUARTIER",
                "codeInsee": grand_quartier_code,
            })

    # 2. ARRONDISSEMENT si Paris/Lyon/Marseille
    if _is_arrondissement(citycode):
        candidates.append({
            "typeLocalisation": "ARRONDISSEMENT",
            "codeInsee": citycode,
        })

    # 3. COMMUNE
    # Pour les arrondissements, on ajoute aussi la commune centrale
    commune_code = citycode
    if dept == "75" and citycode != "75056":
        # On ajoute la commune de Paris en fallback
        candidates.append({
            "typeLocalisation": "COMMUNE",
            "codeInsee": citycode,  # L'arrondissement lui-même comme commune
        })
        candidates.append({
            "typeLocalisation": "COMMUNE",
            "codeInsee": "75056",  # Paris commune centrale
        })
    elif citycode in ("69123", "13055"):
        # Lyon/Marseille commune centrale
        candidates.append({
            "typeLocalisation": "COMMUNE",
            "codeInsee": citycode,
        })
    else:
        candidates.append({
            "typeLocalisation": "COMMUNE",
            "codeInsee": citycode,
        })

    # 4. DEPARTEMENT
    candidates.append({
        "typeLocalisation": "DEPARTEMENT",
        "codeInsee": dept,
    })

    # 5. FRANCE
    candidates.append({
        "typeLocalisation": "FRANCE",
        "codeInsee": None,
    })

    return candidates


def build_notaires_url(location: dict, filters: dict | None = None) -> str:
    """
    Construit l'URL immobilier.notaires.fr à partir d'une localisation et de filtres.

    Args:
        location: Dict avec "typeLocalisation" et "codeInsee"
                  Ex: {"typeLocalisation": "GRAND_QUARTIER", "codeInsee": "9205104"}
        filters: Dict optionnel avec les clés possibles :
                 - typeBien: "APP", "MAI", "TER"
                 - neuf: "A" (ancien), ou absent
                 - piece: int (1-7)
                 - surfaceMin: int
                 - stationnement: int (1)

    Returns:
        URL complète
    """
    params = {}

    params["typeLocalisation"] = location["typeLocalisation"]

    if location.get("codeInsee"):
        params["codeInsee"] = location["codeInsee"]

    if filters:
        if "typeBien" in filters:
            params["typeBien"] = filters["typeBien"]
        if "neuf" in filters:
            params["neuf"] = filters["neuf"]
        if "piece" in filters:
            params["piece"] = str(filters["piece"])
        if "surfaceMin" in filters:
            params["surfaceMin"] = str(filters["surfaceMin"])
        if "stationnement" in filters:
            params["stationnement"] = str(filters["stationnement"])

    return f"{NOTAIRES_BASE_URL}?{urlencode(params)}"


def geocode_address(address: str) -> dict | None:
    """
    Géocode une adresse via l'API officielle data.geopf.fr.

    Returns:
        Dict avec lat, lon, citycode, postcode, city, ou None si échec.
    """
    url = "https://data.geopf.fr/geocodage/search/"
    params = {"q": address, "limit": 1}

    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    if not data.get("features"):
        return None

    feature = data["features"][0]
    props = feature["properties"]
    coords = feature["geometry"]["coordinates"]  # [lon, lat]

    return {
        "latitude": coords[1],
        "longitude": coords[0],
        "citycode": props.get("citycode", props.get("municipalitycode", "")),
        "postcode": props.get("postcode", ""),
        "city": props.get("city", props.get("municipality", "")),
    }


def get_iris_from_coords(lat: float, lon: float) -> str | None:
    """
    Récupère le code IRIS (9 chiffres) à partir de coordonnées GPS.

    Utilise le WFS IGN/Géoplateforme — aucune dépendance geo locale.
    Layer : STATISTICALUNITS.IRISGE:iris_ge

    Returns:
        Code IRIS (ex: "751041304") ou None si non trouvé.
    """
    delta = 0.002  # ~200m de bbox autour du point
    bbox = f"{lon - delta},{lat - delta},{lon + delta},{lat + delta},EPSG:4326"

    url = "https://data.geopf.fr/wfs/ows"
    params = {
        "SERVICE": "WFS",
        "VERSION": "2.0.0",
        "REQUEST": "GetFeature",
        "TYPENAMES": "STATISTICALUNITS.IRISGE:iris_ge",
        "BBOX": bbox,
        "OUTPUTFORMAT": "application/json",
        "COUNT": "10",
    }

    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    features = data.get("features", [])
    if not features:
        return None

    # Si plusieurs IRIS dans la bbox, on prend celui dont le centroïde est le plus proche
    # (approximation simple : on prend le premier — la bbox est petite)
    return features[0]["properties"].get("code_iris")


def generate_notaires_urls(address: str, filters: dict | None = None) -> list[str]:
    """
    Pipeline complet : adresse → géocodage → IRIS WFS → candidats → URLs.

    Args:
        address: Adresse postale complète
        filters: Filtres optionnels (typeBien, neuf, piece, surfaceMin, stationnement)

    Returns:
        Liste d'URLs du plus précis au plus large.
    """
    geo = geocode_address(address)
    if not geo:
        return []

    code_iris = get_iris_from_coords(geo["latitude"], geo["longitude"])

    candidates = build_location_candidates(
        citycode=geo["citycode"],
        code_iris=code_iris,
        postcode=geo["postcode"],
    )

    return [build_notaires_url(loc, filters) for loc in candidates]


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------


def _is_arrondissement(citycode: str) -> bool:
    """Vérifie si un citycode correspond à un arrondissement Paris/Lyon/Marseille."""
    if len(citycode) != 5:
        return False
    # Paris : 75101-75120
    if citycode.startswith("75") and 75101 <= int(citycode) <= 75120:
        return True
    # Lyon : 69381-69389
    if citycode.startswith("69") and 69381 <= int(citycode) <= 69389:
        return True
    # Marseille : 13201-13216
    if citycode.startswith("13") and 13201 <= int(citycode) <= 13216:
        return True
    return False


# ---------------------------------------------------------------------------
# EXEMPLE D'UTILISATION
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    filters = {"typeBien": "APP", "neuf": "A", "piece": 4}

    for address in [
        "12 rue Mouffetard, 75005 Paris",
        "10 avenue de la République, 92120 Montrouge",
    ]:
        print(f"\nAdresse : {address}")
        urls = generate_notaires_urls(address, filters=filters)
        for i, url in enumerate(urls, 1):
            print(f"  {i}. {url}")
