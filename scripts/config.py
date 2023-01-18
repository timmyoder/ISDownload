import pathlib

ROOT = pathlib.Path(__file__).parent
LOG_DIR = ROOT / 'logs'
INPUT_DIR = ROOT / 'input'
OUTPUT_DIR = ROOT / 'output'

WEATHER_OUT_DIR = OUTPUT_DIR / 'weather_data'
RAW_DATA_DIR = WEATHER_OUT_DIR / 'raw'

paths = [OUTPUT_DIR,
         WEATHER_OUT_DIR,
         RAW_DATA_DIR,
         INPUT_DIR,
         LOG_DIR,
         ]

for path in paths:
    if not path.exists():
        path.mkdir()


def log_name(file):
    return LOG_DIR / f'{pathlib.Path(file).name[:-3]}.log'
