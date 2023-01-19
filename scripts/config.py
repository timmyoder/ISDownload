import pathlib

ROOT = pathlib.Path(__file__).parent.parent
LOG_DIR = ROOT / 'logs'
INPUT_DIR = ROOT / 'input'

WEATHER_OUT_DIR = ROOT / 'weather_data_output'
RAW_DATA_DIR = WEATHER_OUT_DIR / 'raw'

paths = [WEATHER_OUT_DIR,
         RAW_DATA_DIR,
         INPUT_DIR,
         LOG_DIR,
         ]

for path in paths:
    if not path.exists():
        path.mkdir()


def log_name(file):
    return LOG_DIR / f'{pathlib.Path(file).name[:-3]}.log'
