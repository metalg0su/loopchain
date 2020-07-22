from pathlib import Path
from typing import Iterable


class FilePath:
    def __init__(self, root_path: Path):
        self._root_path = root_path

    @property
    def channel_manage_data(self) -> Path:
        return self._root_path / "channel_manage_data.json"

    @property
    def peer_configs(self) -> Iterable[Path]:
        config_root: Path = self._root_path / "_tools"
        return sorted(config_root.glob("test_*_conf.json"))
