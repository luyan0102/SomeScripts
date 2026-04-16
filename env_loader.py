import os
from pathlib import Path


def parse_env_line(line: str):
    line = line.strip()
    if not line or line.startswith("#"):
        return None, None

    if line.startswith("export "):
        line = line[len("export ") :].strip()

    key, separator, raw_value = line.partition("=")
    if not separator:
        return None, None

    key = key.strip()
    value = raw_value.strip()
    if not key:
        return None, None

    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        value = value[1:-1]
    else:
        value = value.split(" #", 1)[0].strip()

    return key, value


def load_local_env(env_path: str = ".env", override: bool = False) -> bool:
    path = Path(env_path)
    if not path.is_absolute():
        path = Path.cwd() / env_path
        if not path.exists():
            path = Path(__file__).resolve().parent / env_path

    if not path.exists():
        return False

    for line in path.read_text(encoding="utf-8").splitlines():
        key, value = parse_env_line(line)
        if not key:
            continue
        if override or key not in os.environ:
            os.environ[key] = value

    return True
