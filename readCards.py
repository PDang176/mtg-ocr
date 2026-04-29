import psycopg2
import json
import requests
import re
from PIL import Image
from io import BytesIO
from rapidocr import RapidOCR
from rapidfuzz.distance import Levenshtein
import numpy as np

# ── Fill in your credentials ──────────────────────────────────────────────────
DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "postgres",
    "user": "ddt",
    "password": "Xmac2020@",
}
# ─────────────────────────────────────────────────────────────────────────────

# ── Tuning knobs (single-face cards) ─────────────────────────────────────────
CROP_TOP_PERCENT = 0.10
CROP_LEFT_PX = 10
CROP_RIGHT_PERCENT = 0.75
GRAYSCALE = False
LEV_THRESHOLD = 5

# ── Tuning knobs (double-faced cards) ────────────────────────────────────────
DFC_CROP_TOP_PERCENT = 0.10
DFC_CROP_LEFT_PX = 80
DFC_CROP_RIGHT_PERCENT = 0.70
# ─────────────────────────────────────────────────────────────────────────────

# ── OCR substitution table ────────────────────────────────────────────────────
OCR_SUBSTITUTIONS = {
    "0": "o",
    "1": "l",
    "I": "l",
    "@": "a",
    "8": "B",
    "|": "l",
    "!": "i",
    "$": "s",
    "5": "s",
    "6": "b",
    "9": "g",
    "2": "z",
}
# ─────────────────────────────────────────────────────────────────────────────

engine = RapidOCR()


def clean_ocr_text(raw: str) -> str:
    if not raw:
        return ""
    substituted = "".join(OCR_SUBSTITUTIONS.get(ch, ch) for ch in raw)
    cleaned = re.sub(r"[^a-zA-Z0-9 \-']", "", substituted)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned.lower()


def fetch_and_prepare_image(url, is_dfc=False):
    response = requests.get(url, timeout=10)
    response.raise_for_status()

    img = Image.open(BytesIO(response.content)).convert("RGB")
    width, height = img.size

    if is_dfc:
        left = DFC_CROP_LEFT_PX
        right = int(width * DFC_CROP_RIGHT_PERCENT)
        bottom = int(height * DFC_CROP_TOP_PERCENT)
    else:
        left = CROP_LEFT_PX
        right = int(width * CROP_RIGHT_PERCENT)
        bottom = int(height * CROP_TOP_PERCENT)

    name_bar = img.crop((left, 0, right, bottom))

    if GRAYSCALE:
        name_bar = name_bar.convert("L")

    return np.array(name_bar)


def load_all_card_names(conn):
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT name FROM cards;")
    names = [row[0] for row in cur.fetchall()]
    cur.close()
    print(f"Loaded {len(names):,} card names for matching.")
    return names


def best_levenshtein_match(cleaned_ocr: str, all_names: list):
    if not cleaned_ocr:
        return None, None, False

    best_name = None
    best_dist = float("inf")

    for name in all_names:
        candidate = name.lower().split(" // ")[0].strip()
        candidate = re.sub(r"[^a-zA-Z0-9 \-']", "", candidate)
        dist = Levenshtein.distance(cleaned_ocr, candidate)
        if dist < best_dist:
            best_dist = dist
            best_name = name

    confident = best_dist <= LEV_THRESHOLD
    return best_name, best_dist, confident


def get_normal_uris(cur, limit=100):
    query = """
        SELECT id, name, image_uris, card_faces
        FROM cards
        WHERE image_uris IS NOT NULL
           OR card_faces IS NOT NULL
    """
    if limit:
        query += f" LIMIT {limit}"

    cur.execute(query)
    rows = cur.fetchall()

    results = []
    for card_id, db_name, image_uris, card_faces in rows:
        is_dfc = " // " in (db_name or "")

        if image_uris and image_uris.get("normal"):
            results.append(
                {
                    "id": str(card_id),
                    "name": db_name,
                    "face": None,
                    "normal": image_uris["normal"],
                    "is_dfc": is_dfc,
                }
            )
        elif card_faces:
            for i, face in enumerate(card_faces):
                uris = face.get("image_uris", {})
                if uris.get("normal"):
                    results.append(
                        {
                            "id": str(card_id),
                            "name": db_name,
                            "face": face.get("name", f"face_{i}"),
                            "normal": uris["normal"],
                            "is_dfc": True,
                        }
                    )
    return results


def run_ocr_on_cards(limit=100, save_images=False):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    print("Loading all card names for Levenshtein matching...")
    all_names = load_all_card_names(conn)

    print("\nFetching normal image URLs from database...")
    cards = get_normal_uris(cur, limit=limit)
    cur.close()
    print(f"Found {len(cards)} cards to process.\n")

    ocr_results = []

    for card in cards:
        label = card["name"] + (f" [{card['face']}]" if card["face"] else "")
        url = card["normal"]
        is_dfc = card["is_dfc"]

        print(f"Processing: {label} {'[DFC]' if is_dfc else ''}")
        print(f"  URL:        {url}")

        try:
            img_array = fetch_and_prepare_image(url, is_dfc=is_dfc)

            if save_images:
                safe_name = label.replace(" ", "_").replace("/", "-")[:60]
                Image.fromarray(img_array).save(f"namebar_{safe_name}.jpg")

            result = engine(img_array)
            ocr_raw = " ".join(result.txts).strip() if result.txts else ""

            if not ocr_raw:
                print(f"  ⚠️  OCR returned empty — skipping Levenshtein")
                ocr_results.append(
                    {
                        "id": card["id"],
                        "name": card["name"],
                        "face": card["face"],
                        "ocr_raw": "",
                        "ocr_clean": "",
                        "best_match": None,
                        "edit_dist": None,
                        "confident": False,
                        "correct": False,
                        "is_dfc": is_dfc,
                        "url": url,
                    }
                )
                print()
                continue

            ocr_clean = clean_ocr_text(ocr_raw)
            best_match, edit_dist, confident = best_levenshtein_match(
                ocr_clean, all_names
            )

            correct = best_match == card["name"] or (
                card["face"] and best_match == card["name"].split(" // ")[0]
            )

            print(f"  DB name:    {card['name']}")
            print(f"  OCR raw:    {ocr_raw!r}")
            print(f"  OCR clean:  {ocr_clean!r}")
            print(
                f"  Best match: {best_match}  (distance={edit_dist}, confident={confident})"
            )
            print(f"  Correct:    {'✅' if correct else '❌'}")

            ocr_results.append(
                {
                    "id": card["id"],
                    "name": card["name"],
                    "face": card["face"],
                    "ocr_raw": ocr_raw,
                    "ocr_clean": ocr_clean,
                    "best_match": best_match,
                    "edit_dist": edit_dist,
                    "confident": confident,
                    "correct": correct,
                    "is_dfc": is_dfc,
                    "url": url,
                }
            )

        except Exception as e:
            print(f"  ❌ Error: {e}")
            ocr_results.append(
                {
                    "id": card["id"],
                    "name": card["name"],
                    "face": card["face"],
                    "ocr_raw": None,
                    "ocr_clean": None,
                    "best_match": None,
                    "edit_dist": None,
                    "confident": False,
                    "correct": False,
                    "is_dfc": is_dfc,
                    "error": str(e),
                    "url": url,
                }
            )

        print()

    # Summary
    total = len(ocr_results)
    correct = sum(1 for r in ocr_results if r.get("correct"))
    confident = sum(1 for r in ocr_results if r.get("confident"))
    empty = sum(1 for r in ocr_results if r.get("ocr_raw") == "")
    dfc_empty = sum(
        1 for r in ocr_results if r.get("ocr_raw") == "" and r.get("is_dfc")
    )

    print(f"✅ Correct:      {correct}/{total}")
    print(f"🎯 Confident:    {confident}/{total}  (edit distance ≤ {LEV_THRESHOLD})")
    print(f"⚠️  Empty OCR:   {empty}/{total}  ({dfc_empty} were DFC)")
    print(f"❌ Wrong:        {total - correct}/{total}")

    with open("ocr_names.json", "w") as f:
        json.dump(ocr_results, f, indent=2)
    print("\nResults saved to ocr_names.json")

    return ocr_results


if __name__ == "__main__":
    run_ocr_on_cards(
        limit=100,
        save_images=False,  # set True to save each cropped name bar to disk
    )
