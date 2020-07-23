from pathlib import Path

from loopchain import configure as conf
from loopchain import utils


class Test:
    STORE_ROOT = ".storage"

    def test_rename_db_dir(self, tmp_path: Path, monkeypatch):
        storage_root = tmp_path / self.STORE_ROOT
        monkeypatch.setattr(conf, "DEFAULT_STORAGE_PATH", storage_root)

        old_store_name = "db_111.111.111.111:7200_icon_dex"
        new_store_name = "i_want_to_rename_my_db"

        # GIVEN I already have a db storage
        orig_db_path = Path(storage_root / old_store_name)
        orig_db_path.mkdir(parents=True)
        assert orig_db_path.exists()

        # AND I didn't change its name
        expected_db_path = Path(storage_root / new_store_name)
        assert not expected_db_path.exists()

        # WHEN I rename my db storage name
        utils.rename_db_dir(old_store_name, new_store_name)

        # THEN it should be renamed
        assert not orig_db_path.exists()
        assert expected_db_path.exists()
