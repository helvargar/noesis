import html
import re
from typing import List, Optional, Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


class MuseumBroker:
    """
    Business Logic Layer that abstracts database access.
    Implements the logic defined in the SpringBoot REST API (Swagger).
    """
    def __init__(self, engine: Engine, schema: str = "guide"):
        self.engine = engine
        self.schema = schema
        self.category_map = {
            "it": {"SCULTORI": "Scultori", "PITTORI": "Pittori", "DIRETTORI": "Direttori"},
            "en": {"SCULTORI": "Sculptors", "PITTORI": "Painters", "DIRETTORI": "Directors"},
            "fr": {"SCULTORI": "Sculpteurs", "PITTORI": "Peintres", "DIRETTORI": "Directeurs"},
            "es": {"SCULTORI": "Escultores", "PITTORI": "Pintores", "DIRETTORI": "Directores"}
        }

    def _localize_category(self, category: Optional[str], lang: str) -> str:
        if not category:
            return ""
        upper_cat = category.upper()
        return self.category_map.get(lang, self.category_map["it"]).get(upper_cat, category)

    def _strip_html(self, text_str: Optional[str]) -> str:
        if not text_str:
            return ""
        text_str = html.unescape(text_str)
        text_str = re.sub(r'<[^>]+>', ' ', text_str)
        text_str = re.sub(r'\s+', ' ', text_str).strip()
        return text_str

    # ------------------------------------------------------------------
    # OPERE
    # ------------------------------------------------------------------

    def list_opere(
        self,
        site_id: int,
        title: Optional[str] = None,
        artist_name: Optional[str] = None,
        artist_category: Optional[str] = None,
        room_name: Optional[str] = None,
        technique: Optional[str] = None,
        general_query: Optional[str] = None,
        include_sensoriale: bool = False,
    ) -> List[Dict[str, Any]]:
        """Lista opere con filtri. Esclude versioni Sensoriale per default."""
        query = f"""
            SELECT aw.artistworkid, aw.artistworktitle, a.artistname,
                   ac.artistcategorydescription, r.roomname, t.techniquedescription
            FROM {self.schema}.artistwork aw
            LEFT JOIN {self.schema}.artist a ON aw.artistid = a.artistid
            LEFT JOIN {self.schema}.artistcategory ac ON a.artistcategoryid = ac.artistcategoryid
            LEFT JOIN {self.schema}.room r ON aw.roomid = r.roomid
            LEFT JOIN {self.schema}.technique t ON aw.techniqueid = t.techniqueid
            WHERE aw.siteid = :site_id
        """
        params: Dict[str, Any] = {"site_id": site_id}

        # Exclude "Sensoriale" reproductions unless explicitly requested
        if not include_sensoriale:
            query += " AND aw.artistworktitle NOT ILIKE '%Sensoriale%'"

        if title:
            stop_titles = {"il", "lo", "la", "i", "gli", "le", "un", "uno", "una",
                           "del", "della", "dei", "degli", "delle", "di", "con", "in"}
            title_terms = [t for t in title.split() if t.lower() not in stop_titles and len(t) > 1]
            if title_terms:
                # Try strict match first
                strict_conditions = []
                for i, term in enumerate(title_terms):
                    key = f"title_s_{i}"
                    strict_conditions.append(f"aw.artistworktitle ILIKE :{key}")
                    params[key] = f"%{term}%"
                
                # We add a 'relevance' score based on how many terms match
                # This is a DIY fuzzy search using weighted OR if AND fails
                or_conditions = []
                for i, term in enumerate(title_terms):
                    key = f"title_o_{i}"
                    or_conditions.append(f"CASE WHEN aw.artistworktitle ILIKE :{key} THEN 1 ELSE 0 END")
                    params[key] = f"%{term}%"
                
                relevance_sql = " + ".join(or_conditions)
                query += f" AND ({' AND '.join(strict_conditions)} OR ({relevance_sql}) > 0)"
                # We'll use this relevance in ORDER BY later if we want
            else:
                query += " AND aw.artistworktitle ILIKE :title_fallback"
                params["title_fallback"] = f"%{title}%"

        if artist_name:
            name_terms = [t for t in artist_name.split()
                          if t.lower() not in {"di", "da", "del", "de"} and len(t) > 2]
            if name_terms:
                conditions = []
                for i, term in enumerate(name_terms):
                    key = f"art_n_{i}"
                    conditions.append(f"a.artistname ILIKE :{key}")
                    params[key] = f"%{term}%"
                query += f" AND ({' OR '.join(conditions)})"
            else:
                query += " AND a.artistname ILIKE :art_fallback"
                params["art_fallback"] = f"%{artist_name}%"

        if artist_category:
            cat_clean = artist_category.upper()
            if cat_clean in ["SCULTORE", "SCULTORI", "SCULTURA", "SCULTURE"]:
                query += " AND (ac.artistcategorydescription ILIKE :artist_category OR ac.artistcategorydescription = 'SCULTORI')"
            elif cat_clean in ["PITTORE", "PITTORI", "PITTURA", "DIPINTO", "DIPINTI"]:
                query += " AND (ac.artistcategorydescription ILIKE :artist_category OR ac.artistcategorydescription = 'PITTORI')"
            else:
                query += " AND ac.artistcategorydescription ILIKE :artist_category"
            params["artist_category"] = f"%{artist_category}%"

        if room_name:
            query += " AND r.roomname ILIKE :room_name"
            params["room_name"] = f"%{room_name}%"

        if technique:
            tech_clean = technique.upper()
            # STRICT: filter ONLY via technique table — never via free-text description
            if "BRONZ" in tech_clean:
                query += " AND t.techniquedescription ILIKE '%BRONZ%'"
            elif "OLIO" in tech_clean or "OIL" in tech_clean:
                query += " AND (t.techniquedescription ILIKE '%OLIO%' OR t.techniquedescription ILIKE '%OIL%')"
            elif "GESSO" in tech_clean or "PLASTER" in tech_clean:
                query += " AND (t.techniquedescription ILIKE '%GESS%' OR t.techniquedescription ILIKE '%PLASTER%')"
            elif "TERRACOTTA" in tech_clean:
                query += " AND t.techniquedescription ILIKE '%TERRACOTT%'"
            elif "MARMO" in tech_clean or "MARBLE" in tech_clean:
                query += " AND (t.techniquedescription ILIKE '%MARMO%' OR t.techniquedescription ILIKE '%MARBLE%')"
            else:
                query += " AND t.techniquedescription ILIKE :technique"
            params["technique"] = f"%{technique}%"

        if general_query:
            stop_words = {"di", "del", "della", "dei", "degli", "con", "per",
                          "tra", "fra", "nel", "nella", "uno", "una"}
            terms = [t for t in general_query.split() if len(t) > 2 and t.lower() not in stop_words]
            for i, term in enumerate(terms):
                key = f"gq_{i}"
                t_up = term.upper()
                if t_up in ["SCULTURA", "SCULTURE", "SCULTORE", "SCULTORI"]:
                    query += (
                        f" AND (aw.artistworktitle ILIKE :{key}"
                        f" OR aw.artistworkdescription ILIKE :{key}"
                        f" OR a.artistname ILIKE :{key}"
                        f" OR a.biography ILIKE :{key}"
                        f" OR t.techniquedescription ILIKE :{key}"
                        f" OR ac.artistcategorydescription ILIKE :{key}"
                        f" OR ac.artistcategorydescription = 'SCULTORI')"
                    )
                elif t_up in ["DIPINTO", "DIPINTI", "PITTORE", "PITTORI", "PITTURA"]:
                    query += (
                        f" AND (aw.artistworktitle ILIKE :{key}"
                        f" OR aw.artistworkdescription ILIKE :{key}"
                        f" OR a.artistname ILIKE :{key}"
                        f" OR a.biography ILIKE :{key}"
                        f" OR t.techniquedescription ILIKE :{key}"
                        f" OR ac.artistcategorydescription ILIKE :{key}"
                        f" OR ac.artistcategorydescription = 'PITTORI')"
                    )
                else:
                    query += (
                        f" AND (aw.artistworktitle ILIKE :{key}"
                        f" OR aw.artistworkdescription ILIKE :{key}"
                        f" OR a.artistname ILIKE :{key}"
                        f" OR a.biography ILIKE :{key}"
                        f" OR t.techniquedescription ILIKE :{key}"
                        f" OR r.roomname ILIKE :{key}"
                        f" OR ac.artistcategorydescription ILIKE :{key})"
                    )
                params[key] = f"%{term}%"

        query += " ORDER BY aw.artistworktitle LIMIT 50"

        with self.engine.connect() as conn:
            result = conn.execute(text(query), params)
            return [dict(row._mapping) for row in result]

    def search_by_inventory(self, site_id: int, inventory_number: str) -> List[Dict[str, Any]]:
        """Ricerca un'opera tramite numero di inventario."""
        query = f"""
            SELECT aw.artistworkid, aw.artistworktitle, a.artistname, r.roomname
            FROM {self.schema}.artistwork aw
            LEFT JOIN {self.schema}.artist a ON aw.artistid = a.artistid
            LEFT JOIN {self.schema}.room r ON aw.roomid = r.roomid
            WHERE aw.siteid = :site_id AND aw.inventorynumber = :inv
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {"site_id": site_id, "inv": inventory_number})
            return [dict(row._mapping) for row in result]

    def get_opera_details(
        self,
        site_id: int,
        artist_work_id: int,
        language_id: str = 'it',
        audience_target_id: str = 'STD',
    ) -> Dict[str, Any]:
        """Dettaglio opera: descrizione specifica per target/lingua con fallback."""
        query = f"""
            SELECT aw.artistworkid, aw.artistworktitle as original_title,
                   atd.artistworktargetdescription as description,
                   r.roomname, a.artistname, t.techniquedescription,
                   aw.realizationyear, aw.inventorynumber, aw.roomid
            FROM {self.schema}.artistwork aw
            JOIN {self.schema}.artistworkaudiencetargetdesc atd ON aw.artistworkid = atd.artistworkid
            LEFT JOIN {self.schema}.room r ON aw.roomid = r.roomid
            LEFT JOIN {self.schema}.artist a ON aw.artistid = a.artistid
            LEFT JOIN {self.schema}.technique t ON aw.techniqueid = t.techniqueid
            WHERE aw.artistworkid = :id
              AND atd.languageid = :lang
              AND atd.audiencetargetid = :target
        """
        params = {"id": artist_work_id, "lang": language_id, "target": audience_target_id}

        with self.engine.connect() as conn:
            result = conn.execute(text(query), params).mappings().first()
            if result:
                res_dict = dict(result)
                res_dict["description"] = self._strip_html(res_dict.get("description"))
                title_query = (
                    f"SELECT artistworktitle FROM {self.schema}.artistworklang "
                    f"WHERE artistworkid = :id AND languageid = :lang"
                )
                loc_title = conn.execute(text(title_query), {"id": artist_work_id, "lang": language_id}).scalar()
                res_dict["artistworktitle"] = loc_title or res_dict["original_title"]
                res_dict["image_url"] = result.get("imageref")
                rid = res_dict.get("roomid")
                if rid:
                    nearby = self.list_artworks_in_room(site_id or 1, rid)
                    res_dict["nearby_artworks"] = [
                        a["artistworktitle"] for a in nearby if a["artistworkid"] != artist_work_id
                    ]
                return res_dict

            # Fallback 1: artistworklang (specific language)
            query_fallback = f"""
                SELECT awl.artistworktitle, awl.artistworkdescription as description,
                       r.roomname, a.artistname, t.techniquedescription,
                       aw.realizationyear, aw.inventorynumber, aw.imageref
                FROM {self.schema}.artistwork aw
                JOIN {self.schema}.artistworklang awl ON aw.artistworkid = awl.artistworkid
                LEFT JOIN {self.schema}.room r ON aw.roomid = r.roomid
                LEFT JOIN {self.schema}.artist a ON aw.artistid = a.artistid
                LEFT JOIN {self.schema}.technique t ON aw.techniqueid = t.techniqueid
                WHERE aw.artistworkid = :id AND awl.languageid = :lang
            """
            result = conn.execute(text(query_fallback), {"id": artist_work_id, "lang": language_id}).mappings().first()
            if result and result.get("description"):
                res_dict = dict(result)
                res_dict["description"] = self._strip_html(res_dict.get("description"))
                res_dict["image_url"] = result.get("imageref")
                return res_dict
            
            # Fallback 2: ALWAYS try Italian if description is still missing
            if language_id != 'it':
                result = conn.execute(text(query_fallback), {"id": artist_work_id, "lang": 'it'}).mappings().first()
                if result:
                    res_dict = dict(result)
                    res_dict["description"] = self._strip_html(res_dict.get("description"))
                    res_dict["image_url"] = result.get("imageref")
                    res_dict["note"] = "Descrizione disponibile solo in italiano."
                    return res_dict

        return {}

    # ------------------------------------------------------------------
    # ARTISTI
    # ------------------------------------------------------------------

    def list_artisti(
        self,
        site_id: int,
        name: Optional[str] = None,
        category: Optional[str] = None,
        language_id: str = 'it',
    ) -> List[Dict[str, Any]]:
        """Lista artisti con filtri opzionali."""
        query = f"""
            SELECT a.artistid, a.artistname, ac.artistcategorydescription as category
            FROM {self.schema}.artist a
            LEFT JOIN {self.schema}.artistcategory ac ON a.artistcategoryid = ac.artistcategoryid
            WHERE a.siteid = :site_id
        """
        params: Dict[str, Any] = {"site_id": site_id}

        if name:
            name_terms = [t for t in name.split() if t.lower() not in {"il", "lo", "la", "di", "de", "da"}]
            if name_terms:
                # Use a similarity-like approach: match any term, but prefer those matching more
                conditions = []
                for i, term in enumerate(name_terms):
                    key = f"name_{i}"
                    conditions.append(f"a.artistname ILIKE :{key}")
                    params[key] = f"%{term}%"
                query += f" AND ({' OR '.join(conditions)})"
            else:
                query += " AND a.artistname ILIKE :name_raw"
                params["name_raw"] = f"%{name}%"

        if category:
            cat_clean = category.upper()
            if cat_clean in ["SCULTORE", "SCULTORI", "SCULTURA"]:
                query += " AND (ac.artistcategorydescription ILIKE :category OR ac.artistcategorydescription = 'SCULTORI')"
            elif cat_clean in ["PITTORE", "PITTORI", "PITTURA"]:
                query += " AND (ac.artistcategorydescription ILIKE :category OR ac.artistcategorydescription = 'PITTORI')"
            else:
                query += " AND ac.artistcategorydescription ILIKE :category"
            params["category"] = f"%{category}%"

        query += " ORDER BY a.artistname"

        with self.engine.connect() as conn:
            result = conn.execute(text(query), params)
            artisti = []
            for row in result:
                d = dict(row._mapping)
                if d.get("category"):
                    d["category"] = self._localize_category(d["category"], language_id)
                artisti.append(d)
            return artisti

    def get_artista_details(self, artist_id: int, language_id: str = 'it') -> Dict[str, Any]:
        """
        Dettaglio artista con biografia garantita.
        Restituisce sempre 'biography' (dal campo artist.biography) e
        'description' (dalla tabella artistdescription, con fallback su biography).
        """
        # Step 1: base data always available from artist table
        base_query = f"""
            SELECT a.artistid, a.artistname, a.birthplace, a.deathplace,
                   a.birthdate, a.deathdate, a.biography,
                   ac.artistcategorydescription as category
            FROM {self.schema}.artist a
            LEFT JOIN {self.schema}.artistcategory ac ON a.artistcategoryid = ac.artistcategoryid
            WHERE a.artistid = :id
        """
        # Step 2: localized description (optional, may be missing)
        desc_query = f"""
            SELECT artistdescription, birthdeathdescription
            FROM {self.schema}.artistdescription
            WHERE artistid = :id AND languageid = :lang
            LIMIT 1
        """
        with self.engine.connect() as conn:
            base = conn.execute(text(base_query), {"id": artist_id}).mappings().first()
            if not base:
                return {}

            res = dict(base)
            # Always clean and expose biography
            res["biography"] = self._strip_html(res.get("biography") or "")

            # Fallback 1: requested language
            loc = conn.execute(text(desc_query), {"id": artist_id, "lang": language_id}).mappings().first()
            if loc and loc.get("artistdescription") and len(loc["artistdescription"]) > 10:
                res["description"] = self._strip_html(loc["artistdescription"])
                res["birthdeathdescription"] = loc.get("birthdeathdescription") or ""
            elif language_id != 'it':
                # Fallback 2: Italian
                loc_it = conn.execute(text(desc_query), {"id": artist_id, "lang": 'it'}).mappings().first()
                if loc_it and loc_it.get("artistdescription"):
                    res["description"] = self._strip_html(loc_it["artistdescription"])
                    res["birthdeathdescription"] = loc_it.get("birthdeathdescription") or ""
                    res["note"] = "Biografia disponibile solo in italiano."
                else:
                    res["description"] = res["biography"]
            else:
                # Fallback 3: biography field
                res["description"] = res["biography"]
                res["birthdeathdescription"] = ""

            return res

    # ------------------------------------------------------------------
    # LOCATIONS
    # ------------------------------------------------------------------

    def list_locations(self, site_id: int) -> List[Dict[str, Any]]:
        """Lista sale ed edifici del museo."""
        query = f"""
            SELECT DISTINCT loc.locationid, loc.locationname, r.roomname
            FROM {self.schema}.location loc
            LEFT JOIN {self.schema}.room r ON loc.roomid = r.roomid
            WHERE loc.siteid = :site_id
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {"site_id": site_id})
            return [dict(row._mapping) for row in result]

    def get_location_details(self, location_id: int, language_id: str = 'it') -> Dict[str, Any]:
        """Recupera descrizione di una sala/location."""
        query = f"""
            SELECT ld.locationname, ld.locationdescription as description
            FROM {self.schema}.locationdescription ld
            WHERE ld.locationid = :id AND ld.languageid = :lang
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {"id": location_id, "lang": language_id}).mappings().first()
            if result:
                res_dict = dict(result)
                res_dict["description"] = self._strip_html(res_dict.get("description"))
                return res_dict
        return {}

    # ------------------------------------------------------------------
    # PERCORSI
    # ------------------------------------------------------------------

    def get_percorso_opere(self, site_id: int, pathway_name: str) -> List[Dict[str, Any]]:
        """Opere di un percorso tematico in ordine di sequenza."""
        query = f"""
            SELECT aw.artistworkid, aw.artistworktitle, a.artistname, ps.sortingsequence
            FROM {self.schema}.pathway p
            JOIN {self.schema}.pathwayspot ps ON p.pathwayid = ps.pathwayid
            JOIN {self.schema}.artistwork aw ON ps.artistworkid = aw.artistworkid
            LEFT JOIN {self.schema}.artist a ON aw.artistid = a.artistid
            WHERE p.pathwayname ILIKE :name AND aw.siteid = :site_id
            ORDER BY ps.sortingsequence
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {"name": f"%{pathway_name}%", "site_id": site_id})
            return [dict(row._mapping) for row in result]

    def list_pathways(self, site_id: int) -> List[Dict[str, Any]]:
        """Elenca i percorsi disponibili."""
        query = f"""
            SELECT pathwayid, pathwayname, pathwaydescription
            FROM {self.schema}.pathway
            WHERE siteid = :site_id
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {"site_id": site_id})
            return [dict(row._mapping) for row in result]

    def get_pathway_details(self, pathway_id: int, language_id: str = 'it') -> Dict[str, Any]:
        """Dettagli e descrizione di un percorso."""
        query = f"""
            SELECT pathwayname, pathwaydescription as description
            FROM {self.schema}.pathwaydescription
            WHERE pathwayid = :id AND languageid = :lang
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {"id": pathway_id, "lang": language_id}).mappings().first()
            if result:
                res = dict(result)
                res["description"] = self._strip_html(res.get("description"))
                return res
        return {}

    # ------------------------------------------------------------------
    # CATEGORIE / TECNICHE / MUSEO
    # ------------------------------------------------------------------

    def list_categories(self, site_id: int) -> List[str]:
        """Categorie artisti presenti nel sito."""
        query = f"""
            SELECT DISTINCT ac.artistcategorydescription
            FROM {self.schema}.artistcategory ac
            JOIN {self.schema}.artist a ON ac.artistcategoryid = a.artistcategoryid
            WHERE a.siteid = :site_id
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {"site_id": site_id})
            return [row[0] for row in result if row[0]]

    def list_techniques(self, site_id: int) -> List[str]:
        """Tecniche utilizzate nelle opere del sito."""
        query = f"""
            SELECT DISTINCT t.techniquedescription
            FROM {self.schema}.technique t
            JOIN {self.schema}.artistwork aw ON t.techniqueid = aw.techniqueid
            WHERE aw.siteid = :site_id
            ORDER BY t.techniquedescription
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {"site_id": site_id})
            return [row[0] for row in result if row[0]]

    def get_museum_info(self, site_id: int) -> Dict[str, Any]:
        """Informazioni istituzionali del museo."""
        query = f"""
            SELECT sitename, sitedescription, history, architecture,
                   address, city, country, telephone, email
            FROM {self.schema}.site
            WHERE siteid = :site_id
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {"site_id": site_id}).mappings().first()
            if result:
                res = dict(result)
                res["sitedescription"] = self._strip_html(res.get("sitedescription"))
                res["history"] = self._strip_html(res.get("history"))
                res["architecture"] = self._strip_html(res.get("architecture"))
                return res
        return {}

    def list_artworks_in_room(self, site_id: int, room_id: int) -> List[Dict[str, Any]]:
        """Opere nella stessa sala (max 5)."""
        query = f"""
            SELECT aw.artistworkid, aw.artistworktitle, a.artistname
            FROM {self.schema}.artistwork aw
            LEFT JOIN {self.schema}.artist a ON aw.artistid = a.artistid
            WHERE aw.siteid = :site_id AND aw.roomid = :room_id
            LIMIT 5
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {"site_id": site_id, "room_id": room_id})
            return [dict(row._mapping) for row in result]
