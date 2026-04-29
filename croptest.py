import requests
from PIL import Image
from io import BytesIO

# ── Paste a few DFC normal image URLs here to diagnose ───────────────────────
DFC_URLS = [
    "https://cards.scryfall.io/normal/front/5/5/55b4ae98-86b2-4725-ad99-8571ae50792c.jpg?1679452328",
    # add more here
]

# Try different crop combos and save them all so you can see what works
CROP_COMBOS = [
    {"label": "current", "left": 80, "right": 0.70, "top": 0.10},
    {"label": "more_left", "left": 120, "right": 0.65, "top": 0.10},
    {"label": "taller", "left": 80, "right": 0.70, "top": 0.13},
    {"label": "less_left", "left": 50, "right": 0.75, "top": 0.10},
]

for url in DFC_URLS:
    resp = requests.get(url, timeout=10)
    img = Image.open(BytesIO(resp.content)).convert("RGB")
    w, h = img.size
    print(f"\nImage size: {w}x{h}  URL: {url}")

    card_id = url.split("/")[-1].split(".")[0][:12]

    for combo in CROP_COMBOS:
        left = combo["left"]
        right = int(w * combo["right"])
        bottom = int(h * combo["top"])
        crop = img.crop((left, 0, right, bottom))
        fname = f"dfc_{card_id}_{combo['label']}.jpg"
        crop.save(fname)
        print(f"  Saved {fname}  ({crop.size[0]}x{crop.size[1]}px)")

print("\nDone — open the saved images to see which crop hits the name cleanly.")
