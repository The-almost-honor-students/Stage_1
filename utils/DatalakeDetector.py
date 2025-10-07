def detect_datalake_root(start=None, folder_name="datalake"):
    import os
    from pathlib import Path

    env = os.environ.get("DATALAKE_ROOT")
    if env:
        p = Path(env).expanduser()
        if p.is_dir():
            return p.resolve()

    start = Path.cwd() if start is None else Path(start)
    for base in (start, *start.parents):
        candidate = base / folder_name
        if candidate.is_dir():
            return candidate.resolve()

    home_candidate = Path.home() / folder_name
    if home_candidate.is_dir():
        return home_candidate.resolve()

    root_candidate = Path(os.sep) / folder_name
    if root_candidate.is_dir():
        return root_candidate.resolve()

    raise FileNotFoundError("No se encontró la carpeta 'datalake'. Define DATALAKE_ROOT o crea la carpeta.")
