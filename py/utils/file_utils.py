from pathlib import Path
import subprocess
from console_logging.console import Console
LOG = Console()

def verifyFile(files_list):
    return Path(files_list).is_file()

def verifyType(file_name):
    if Path(file_name).is_dir():
        return "dir"
    elif Path(file_name).is_file():
        return "file"
    else:
        return None

def verifyDir(dir_path):
    if not Path(dir_path).exists():
        Path(dir_path).mkdir(parents=True, mode=0o770, exist_ok=True)

def get_current_path():
    return str(Path(__file__).parent.resolve())

def get_absolute_path(relative_path):
    path = Path(relative_path).absolute().as_posix()
    LOG.success('LOAD PATH {:s}'.format(path))
    return path

def extract_archive(file_path: Path):
    parent_dir = file_path.parent

    # folder name without extension
    folder_name = file_path.stem

    # special case: .sql.gz → remove both extensions
    if file_path.name.endswith(".sql.gz"):
        folder_name = file_path.name.replace(".sql.gz", "")

    target_dir = parent_dir / folder_name
    target_dir.mkdir(exist_ok=True)

    if file_path.suffix == ".zip":
        command = [
            "unzip",
            "-o",              # overwrite if exists
            str(file_path),
            "-d",
            str(target_dir)
        ]

    elif file_path.suffix == ".gz":
        command = [
            "gunzip",
            "-k",
            str(file_path)
        ]

    else:
        return

    try:
        subprocess.run(
            command,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        print(f"Error extracting {file_path}")
        print(e.stderr)
