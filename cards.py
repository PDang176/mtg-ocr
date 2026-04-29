import json
import psycopg2
from psycopg2.extras import execute_values, Json

# This section you need to your credentials for postgres instance

DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "postgres",
    "user": "patrick",
    "password": "yourpassword",
}


# IMPORTANT this is the file name of what you should nanme the downloaded file name
# Scryfall
JSON_FILE_PATH = "scryfall_data.json"
BATCH_SIZE = 1000

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS cards (
    -- Identity
    id                  uuid PRIMARY KEY,
    oracle_id           uuid,
    multiverse_ids      integer[],
    mtgo_id             integer,
    mtgo_foil_id        integer,
    tcgplayer_id        integer,
    tcgplayer_etched_id integer,
    cardmarket_id       integer,
    lang                text,
    released_at         date,
    uri                 text,
    scryfall_uri        text,

    -- Layout / image
    layout              text,
    highres_image       boolean,
    image_status        text,
    image_uris          jsonb,

    -- Gameplay
    name                text NOT NULL,
    mana_cost           text,
    cmc                 numeric,
    type_line           text,
    oracle_text         text,
    power               text,
    toughness           text,
    loyalty             text,
    defense             text,
    colors              text[],
    color_identity      text[],
    color_indicator     text[],
    keywords            text[],
    produced_mana       text[],
    reserved            boolean,
    game_changer        boolean,
    legalities          jsonb,
    all_parts           jsonb,
    card_faces          jsonb,

    -- Print
    artist              text,
    artist_ids          uuid[],
    illustration_id     uuid,
    flavor_text         text,
    flavor_name         text,
    watermark           text,
    border_color        text,
    frame               text,
    frame_effects       text[],
    security_stamp      text,
    full_art            boolean,
    textless            boolean,
    oversized           boolean,
    booster             boolean,
    digital             boolean,
    foil                boolean,
    nonfoil             boolean,
    finishes            text[],
    games               text[],
    promo               boolean,
    promo_types         text[],
    reprint             boolean,
    variation           boolean,
    variation_of        uuid,
    story_spotlight     boolean,
    collector_number    text,
    rarity              text,
    card_back_id        uuid,

    -- Set
    set_id              uuid,
    set_code            text,
    set_name            text,
    set_type            text,
    set_uri             text,
    set_search_uri      text,
    scryfall_set_uri    text,
    rulings_uri         text,
    prints_search_uri   text,

    -- Rankings
    edhrec_rank         integer,
    penny_rank          integer,
    hand_modifier       text,
    life_modifier       text,

    -- Nested objects as jsonb
    prices              jsonb,
    purchase_uris       jsonb,
    related_uris        jsonb,
    preview             jsonb
);

CREATE INDEX IF NOT EXISTS idx_cards_name_fts
    ON cards USING gin(to_tsvector('english', name));
CREATE INDEX IF NOT EXISTS idx_cards_colors
    ON cards USING gin(colors);
CREATE INDEX IF NOT EXISTS idx_cards_color_identity
    ON cards USING gin(color_identity);
CREATE INDEX IF NOT EXISTS idx_cards_keywords
    ON cards USING gin(keywords);
CREATE INDEX IF NOT EXISTS idx_cards_legalities
    ON cards USING gin(legalities);
CREATE INDEX IF NOT EXISTS idx_cards_cmc
    ON cards (cmc);
CREATE INDEX IF NOT EXISTS idx_cards_set_code
    ON cards (set_code);
CREATE INDEX IF NOT EXISTS idx_cards_rarity
    ON cards (rarity);
CREATE INDEX IF NOT EXISTS idx_cards_type_line
    ON cards USING gin(to_tsvector('english', coalesce(type_line, '')));
"""

INSERT_SQL = """
    INSERT INTO cards (
        id, oracle_id, multiverse_ids, mtgo_id, mtgo_foil_id,
        tcgplayer_id, tcgplayer_etched_id, cardmarket_id, lang, released_at,
        uri, scryfall_uri, layout, highres_image, image_status, image_uris,
        name, mana_cost, cmc, type_line, oracle_text, power, toughness,
        loyalty, defense, colors, color_identity, color_indicator, keywords,
        produced_mana, reserved, game_changer, legalities, all_parts, card_faces,
        artist, artist_ids, illustration_id, flavor_text, flavor_name, watermark,
        border_color, frame, frame_effects, security_stamp, full_art, textless,
        oversized, booster, digital, foil, nonfoil, finishes, games, promo,
        promo_types, reprint, variation, variation_of, story_spotlight,
        collector_number, rarity, card_back_id,
        set_id, set_code, set_name, set_type, set_uri, set_search_uri,
        scryfall_set_uri, rulings_uri, prints_search_uri,
        edhrec_rank, penny_rank, hand_modifier, life_modifier,
        prices, purchase_uris, related_uris, preview
    ) VALUES %s
    ON CONFLICT (id) DO UPDATE SET
        prices      = EXCLUDED.prices,
        legalities  = EXCLUDED.legalities,
        edhrec_rank = EXCLUDED.edhrec_rank,
        penny_rank  = EXCLUDED.penny_rank;
"""


def prepare_row(card):
    def j(val):
        return Json(val) if val else None

    return (
        card.get("id"),
        card.get("oracle_id"),
        card.get("multiverse_ids"),
        card.get("mtgo_id"),
        card.get("mtgo_foil_id"),
        card.get("tcgplayer_id"),
        card.get("tcgplayer_etched_id"),
        card.get("cardmarket_id"),
        card.get("lang"),
        card.get("released_at"),
        card.get("uri"),
        card.get("scryfall_uri"),
        card.get("layout"),
        card.get("highres_image"),
        card.get("image_status"),
        j(card.get("image_uris")),
        card.get("name"),
        card.get("mana_cost"),
        card.get("cmc"),
        card.get("type_line"),
        card.get("oracle_text"),
        card.get("power"),
        card.get("toughness"),
        card.get("loyalty"),
        card.get("defense"),
        card.get("colors"),
        card.get("color_identity"),
        card.get("color_indicator"),
        card.get("keywords"),
        card.get("produced_mana"),
        card.get("reserved"),
        card.get("game_changer"),
        j(card.get("legalities")),
        j(card.get("all_parts")),
        j(card.get("card_faces")),
        card.get("artist"),
        card.get("artist_ids"),
        card.get("illustration_id"),
        card.get("flavor_text"),
        card.get("flavor_name"),
        card.get("watermark"),
        card.get("border_color"),
        card.get("frame"),
        card.get("frame_effects"),
        card.get("security_stamp"),
        card.get("full_art"),
        card.get("textless"),
        card.get("oversized"),
        card.get("booster"),
        card.get("digital"),
        card.get("foil"),
        card.get("nonfoil"),
        card.get("finishes"),
        card.get("games"),
        card.get("promo"),
        card.get("promo_types"),
        card.get("reprint"),
        card.get("variation"),
        card.get("variation_of"),
        card.get("story_spotlight"),
        card.get("collector_number"),
        card.get("rarity"),
        card.get("card_back_id"),
        card.get("set_id"),
        card.get("set"),  # Scryfall field is "set" maps to set_code
        card.get("set_name"),
        card.get("set_type"),
        card.get("set_uri"),
        card.get("set_search_uri"),
        card.get("scryfall_set_uri"),
        card.get("rulings_uri"),
        card.get("prints_search_uri"),
        card.get("edhrec_rank"),
        card.get("penny_rank"),
        card.get("hand_modifier"),
        card.get("life_modifier"),
        j(card.get("prices")),
        j(card.get("purchase_uris")),
        j(card.get("related_uris")),
        j(card.get("preview")),
    )


def test_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✅ Connection established.")
        cur = conn.cursor()
        cur.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
        )
        tables = cur.fetchall()
        print(f"Tables in '{DB_CONFIG['database']}':")
        for t in tables:
            print(f"  - {t[0]}")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Connection failed: {e}")


def bulk_insert_cards():
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        print("Creating table and indexes if needed...")
        cur.execute(CREATE_TABLE_SQL)
        conn.commit()

        print(f"Loading {JSON_FILE_PATH}...")
        with open(JSON_FILE_PATH, "r", encoding="utf-8") as f:
            cards_data = json.load(f)

        total = len(cards_data)
        print(f"Loaded {total:,} cards. Inserting in batches of {BATCH_SIZE}...")

        inserted = 0
        for i in range(0, total, BATCH_SIZE):
            batch = cards_data[i : i + BATCH_SIZE]
            rows = [prepare_row(card) for card in batch]
            execute_values(cur, INSERT_SQL, rows, page_size=BATCH_SIZE)
            conn.commit()
            inserted += len(batch)
            print(f"  {inserted:,} / {total:,} inserted...")

        print(f"✅ Done. {inserted:,} cards inserted/updated.")

    except Exception as e:
        print(f"❌ Error: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
