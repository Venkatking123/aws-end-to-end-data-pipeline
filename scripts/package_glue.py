"""Build the dependency-free shared package consumed by --extra-py-files."""

from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

ROOT = Path(__file__).resolve().parents[1]


def package() -> Path:
    destination = ROOT / "dist" / "retail_pipeline.zip"
    destination.parent.mkdir(exist_ok=True)
    with ZipFile(destination, "w", ZIP_DEFLATED) as archive:
        for source in sorted((ROOT / "src" / "retail_pipeline").glob("*.py")):
            info = ZipInfo(f"retail_pipeline/{source.name}", date_time=(2026, 1, 1, 0, 0, 0))
            info.compress_type = ZIP_DEFLATED
            archive.writestr(info, source.read_bytes())
    return destination


if __name__ == "__main__":
    print(package())
