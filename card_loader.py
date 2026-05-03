import psycopg2

from config import Config

class CardLoader:
    def __init__(self, config: Config):
        self.conn = psycopg2.connect(**config.DB_CONFIG)

    def load_all_names(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT DISTINCT name FROM cards;")
            return [row[0] for row in cur.fetchall()]

    def fetch_cards(self, limit=100):
        query = """
            SELECT id, name, image_uris, card_faces
            FROM cards
            WHERE image_uris IS NOT NULL
               OR card_faces IS NOT NULL
        """

        if limit:
            query += f" LIMIT {limit}"

        with self.conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        results = []

        for card_id, name, image_uris, faces in rows:
            is_dfc = " // " in (name or "")

            if image_uris and image_uris.get("normal"):
                results.append({
                    "id": str(card_id),
                    "name": name,
                    "face": None,
                    "url": image_uris["normal"],
                    "is_dfc": is_dfc,
                })
            elif faces:
                for i, face in enumerate(faces):
                    uris = face.get("image_uris", {})
                    if uris.get("normal"):
                        results.append({
                            "id": str(card_id),
                            "name": name,
                            "face": face.get("name", f"face_{i}"),
                            "url": uris["normal"],
                            "is_dfc": True,
                        })

        return results