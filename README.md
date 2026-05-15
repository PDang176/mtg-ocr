# Magic the Gathering OCR Classifier

## Prerequisites

1. Set up Postgresql for your device

## Setup

1. Set up python venv
2. Install python libraries using `pip install -r requirements.txt`
3. Initialize .env file using template
4. Set up Postgresql Database
    - Download oracle bulk data set from [Scryfall](https://scryfall.com/docs/api/bulk-data)
    - Move bulk data to json folder and rename to `scryfall_data.json`
    - Run create_database.py

## Usage

1. Run ocr_pipeline.py for results
2. Run tests/edgetest.py for use with video feed
