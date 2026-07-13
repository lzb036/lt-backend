from __future__ import annotations

from io import BytesIO
import tempfile
import unittest
from contextlib import contextmanager
from unittest.mock import patch
from zipfile import ZipFile
import json

from sqlalchemy import event
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker

from app.db.database import Base


class SensitiveWordModelTests(unittest.TestCase):
    def test_sensitive_word_model_has_expected_schema(self) -> None:
        from app.services.sensitive_word_service import normalize_sensitive_word
        from app.db.models import SensitiveWordModel

        id_column = SensitiveWordModel.__table__.columns["id"]
        enabled_column = SensitiveWordModel.__table__.columns["enabled"]
        created_at_column = SensitiveWordModel.__table__.columns["created_at"]
        updated_at_column = SensitiveWordModel.__table__.columns["updated_at"]

        self.assertEqual(normalize_sensitive_word("  即納  "), "即納")
        self.assertEqual(SensitiveWordModel.__tablename__, "lt_sensitive_words")
        self.assertIn("id", SensitiveWordModel.__table__.columns)
        self.assertIn("word", SensitiveWordModel.__table__.columns)
        self.assertIn("enabled", SensitiveWordModel.__table__.columns)
        self.assertIn("created_at", SensitiveWordModel.__table__.columns)
        self.assertIn("updated_at", SensitiveWordModel.__table__.columns)
        self.assertTrue(id_column.primary_key)
        self.assertTrue(id_column.autoincrement)
        self.assertEqual(SensitiveWordModel.__table__.columns["word"].type.length, 500)
        self.assertFalse(SensitiveWordModel.__table__.columns["word"].nullable)
        self.assertFalse(enabled_column.nullable)
        self.assertIsNotNone(enabled_column.default)
        self.assertTrue(enabled_column.default.arg)
        self.assertIsNotNone(enabled_column.server_default)
        self.assertEqual(str(enabled_column.server_default.arg), "1")
        self.assertFalse(created_at_column.nullable)
        self.assertIsNotNone(created_at_column.server_default)
        self.assertFalse(updated_at_column.nullable)
        self.assertIsNotNone(updated_at_column.server_default)
        self.assertIsNotNone(updated_at_column.onupdate)
        self.assertTrue(
            any(constraint.name == "uq_lt_sensitive_word" for constraint in SensitiveWordModel.__table__.constraints)
        )


class SensitiveWordSanitizerTests(unittest.TestCase):
    def test_removes_normal_words_and_collapses_whitespace(self) -> None:
        from app.services.sensitive_word_service import sanitize_sensitive_text

        self.assertEqual(
            sanitize_sensitive_text(
                "楽天1位  春物  即納",
                ["楽天1位", "即納"],
            ),
            "春物",
        )

    def test_empty_bracket_rule_removes_every_bracketed_segment(self) -> None:
        from app.services.sensitive_word_service import sanitize_sensitive_text

        self.assertEqual(
            sanitize_sensitive_text(
                "【楽天1位】【日本国内発送】 春物",
                ["【】"],
            ),
            "春物",
        )

    def test_overlapping_literal_words_are_deduplicated_and_applied_longest_first(self) -> None:
        from app.services.sensitive_word_service import sanitize_sensitive_text

        self.assertEqual(
            sanitize_sensitive_text(
                "期間限定",
                [" 限定 ", "期間限定", "限定", "期間限定"],
            ),
            "",
        )

    def test_payload_sanitizer_updates_title_and_tagline_fields_recursively(self) -> None:
        from app.services.sensitive_word_service import sanitize_product_payload

        payload = {
            "title": "【楽天1位】 春物",
            "itemName": "【楽天1位】 春物",
            "tagline": "即納 おすすめ",
            "item": {"subtitle": "【期間限定】 即納"},
            "description": "【期間限定】 即納",
        }

        cleaned, changed = sanitize_product_payload(payload, ["【】", "即納"])

        self.assertTrue(changed)
        self.assertEqual(cleaned["title"], "春物")
        self.assertEqual(cleaned["itemName"], "春物")
        self.assertEqual(cleaned["tagline"], "おすすめ")
        self.assertEqual(cleaned["item"]["subtitle"], "")
        self.assertEqual(cleaned["description"], "【期間限定】 即納")

    def test_payload_sanitizer_only_cleans_root_name_but_keeps_nested_generic_names(self) -> None:
        from app.services.sensitive_word_service import sanitize_product_payload

        payload = {
            "name": "即納 春物",
            "metadata": {
                "name": "即納 サイズ名",
                "tagline": "即納 おすすめ",
            },
            "attributes": [
                {"name": "即納 カラー", "value": "赤"},
                {"name": "即納 素材", "value": "綿"},
            ],
            "item": {
                "itemName": "即納 ワンピース",
                "subtitle": "即納 人気",
            },
        }

        cleaned, changed = sanitize_product_payload(payload, ["即納"])

        self.assertTrue(changed)
        self.assertEqual(cleaned["name"], "春物")
        self.assertEqual(cleaned["metadata"]["name"], "即納 サイズ名")
        self.assertEqual(cleaned["attributes"][0]["name"], "即納 カラー")
        self.assertEqual(cleaned["attributes"][1]["name"], "即納 素材")
        self.assertEqual(cleaned["metadata"]["tagline"], "おすすめ")
        self.assertEqual(cleaned["item"]["itemName"], "ワンピース")
        self.assertEqual(cleaned["item"]["subtitle"], "人気")


class SensitiveWordDatabaseTestCase(unittest.TestCase):
    def setUp(self) -> None:
        import app.db.models  # noqa: F401

        self.engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        Base.metadata.create_all(self.engine)
        self.session_factory = sessionmaker(
            bind=self.engine,
            expire_on_commit=False,
            future=True,
        )

    def tearDown(self) -> None:
        self.engine.dispose()

    @contextmanager
    def session_scope(self):
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def list_words(self) -> list[str]:
        from app.db.models import SensitiveWordModel

        with Session(self.engine, future=True) as session:
            return session.scalars(
                select(SensitiveWordModel.word).order_by(SensitiveWordModel.id.asc())
            ).all()


class SensitiveWordPersistenceTests(SensitiveWordDatabaseTestCase):
    def test_seed_is_idempotent_and_deduplicates_default_words(self) -> None:
        from app.db.models import SensitiveWordModel
        from app.services.sensitive_word_service import DEFAULT_SENSITIVE_WORDS, seed_default_sensitive_words

        expected_default_words = (
            "500円OFFクーポン",
            "【全店2点購入で10％OFF】",
            "300円OFFクーポン",
            "【お買い物マラソン 当店ポイント5倍】",
            "翌日出荷",
            "翌日配達",
            "楽天1位",
            "楽天2位",
            "楽天3位",
            "【】",
            "【P10&最大600円OFF】",
            "【楽天倉庫出荷】",
            "【日本国内発送】",
            "【楽天1位】",
            "【楽天2位】",
            "【楽天3位】",
            "新生活",
            "即納",
            "【10%OFFクーポン】",
            "【8%OFFクーポン】",
            "【期間限定5％OFF】",
            "一部即納",
            "【P5倍期間限定】",
            "【P10倍期間限定】",
            "お買い物マラソン",
            "【P5倍】",
            "【LINE追加で5%OFF】",
            "【限定10%OFF】",
            "【スーパーDEAL&お買い物マラソンP10】",
            "【お買い物マラソン最大2000円OFF】",
            "＼7%OFFクーポン利用可2.18まで／",
            "【短納期】",
        )

        self.assertEqual(DEFAULT_SENSITIVE_WORDS, expected_default_words)

        with self.session_scope() as session:
            created_count = seed_default_sensitive_words(session)
            self.assertGreater(created_count, 0)

        with self.session_scope() as session:
            self.assertEqual(seed_default_sensitive_words(session), 0)
            total = session.scalar(select(func.count()).select_from(SensitiveWordModel))

        self.assertEqual(total, len(set(DEFAULT_SENSITIVE_WORDS)))
        self.assertEqual(len(DEFAULT_SENSITIVE_WORDS), len(set(DEFAULT_SENSITIVE_WORDS)))
        self.assertEqual(list(self.list_words()), list(expected_default_words))
        self.assertIn("【】", DEFAULT_SENSITIVE_WORDS)
        self.assertIn("即納", DEFAULT_SENSITIVE_WORDS)
        self.assertIn("楽天1位", DEFAULT_SENSITIVE_WORDS)
        self.assertIn("翌日配達", DEFAULT_SENSITIVE_WORDS)

    def test_init_database_seeds_defaults_without_breaking_existing_bootstrap_steps(self) -> None:
        import app.db.database as database_module
        from app.db.models import SensitiveWordModel

        with (
            patch.object(database_module, "engine", self.engine),
            patch.object(database_module, "SessionLocal", self.session_factory),
            patch.object(database_module, "ensure_mysql_database_exists") as ensure_database_exists,
            patch.object(database_module, "ensure_schema_compatibility") as ensure_schema_compatibility,
            patch.object(database_module.settings, "database_auto_create", False),
            patch("app.services.user_service.ensure_initial_superadmin") as ensure_initial_superadmin,
            patch("app.services.crawler_service.ensure_default_roles") as ensure_default_roles,
        ):
            database_module.init_database()

        ensure_database_exists.assert_not_called()
        ensure_schema_compatibility.assert_called_once_with()
        ensure_initial_superadmin.assert_called_once_with()
        ensure_default_roles.assert_called_once_with()

        with Session(self.engine, future=True) as session:
            total = session.scalar(select(func.count()).select_from(SensitiveWordModel))
            self.assertEqual(total, session.scalar(select(func.count()).select_from(SensitiveWordModel)))
            self.assertGreater(total or 0, 0)

        with (
            patch.object(database_module, "engine", self.engine),
            patch.object(database_module, "SessionLocal", self.session_factory),
            patch.object(database_module, "ensure_mysql_database_exists"),
            patch.object(database_module, "ensure_schema_compatibility"),
            patch.object(database_module.settings, "database_auto_create", False),
            patch("app.services.user_service.ensure_initial_superadmin"),
            patch("app.services.crawler_service.ensure_default_roles"),
        ):
            database_module.init_database()

        with Session(self.engine, future=True) as session:
            second_total = session.scalar(select(func.count()).select_from(SensitiveWordModel))

        self.assertEqual(second_total, total)

    def test_crud_normalizes_words_and_rejects_duplicates(self) -> None:
        from app.services import sensitive_word_service

        with patch.object(sensitive_word_service, "SessionLocal", self.session_factory):
            created = sensitive_word_service.create_sensitive_word("  即納  ")

            self.assertEqual(created["word"], "即納")
            self.assertEqual(created["ruleType"], "literal")
            self.assertTrue(created["enabled"])

            with self.assertRaisesRegex(RuntimeError, "已存在"):
                sensitive_word_service.create_sensitive_word("即納")

            with self.assertRaisesRegex(RuntimeError, "不能为空"):
                sensitive_word_service.create_sensitive_word("   ")

    def test_list_filters_and_paginates_sensitive_words(self) -> None:
        from app.services import sensitive_word_service

        with patch.object(sensitive_word_service, "SessionLocal", self.session_factory):
            sensitive_word_service.create_sensitive_word("翌日配達")
            sensitive_word_service.create_sensitive_word("即納")
            sensitive_word_service.create_sensitive_word("【】")

            page_one = sensitive_word_service.list_sensitive_words(page=1, page_size=2)
            filtered = sensitive_word_service.list_sensitive_words(page=1, page_size=10, keyword="即")

        self.assertEqual(page_one["total"], 3)
        self.assertEqual(page_one["page"], 1)
        self.assertEqual(page_one["pageSize"], 2)
        self.assertEqual(len(page_one["items"]), 2)
        self.assertEqual([item["word"] for item in page_one["items"]], ["翌日配達", "即納"])
        self.assertEqual(filtered["total"], 1)
        self.assertEqual(filtered["items"][0]["word"], "即納")
        self.assertEqual(filtered["items"][0]["ruleType"], "literal")

    def test_update_delete_and_active_word_ordering_respect_enabled_state(self) -> None:
        from app.db.models import SensitiveWordModel
        from app.services import sensitive_word_service

        with patch.object(sensitive_word_service, "SessionLocal", self.session_factory):
            created_bracket = sensitive_word_service.create_sensitive_word("【】")
            created_short = sensitive_word_service.create_sensitive_word("即納")
            created_long = sensitive_word_service.create_sensitive_word("期間限定")

            updated = sensitive_word_service.update_sensitive_word(created_short["id"], "  翌日配達  ", False)
            self.assertEqual(updated["word"], "翌日配達")
            self.assertFalse(updated["enabled"])

            with self.assertRaisesRegex(RuntimeError, "已存在"):
                sensitive_word_service.update_sensitive_word(created_long["id"], "【】", True)

            self.assertTrue(sensitive_word_service.delete_sensitive_word(created_bracket["id"]))
            self.assertFalse(sensitive_word_service.delete_sensitive_word(created_bracket["id"]))

        with Session(self.engine, future=True) as session:
            active_words = sensitive_word_service.active_sensitive_words(session)
            rows = session.scalars(select(SensitiveWordModel).order_by(SensitiveWordModel.id.asc())).all()

        self.assertEqual(active_words, ["期間限定"])
        self.assertEqual([row.word for row in rows], ["翌日配達", "期間限定"])
        self.assertFalse(rows[0].enabled)
        self.assertTrue(rows[1].enabled)


class SensitiveWordExcelImportTests(SensitiveWordDatabaseTestCase):
    def test_template_contains_expected_sheet_and_header(self) -> None:
        from openpyxl import load_workbook

        from app.services.sensitive_word_service import build_sensitive_word_template

        content = build_sensitive_word_template()
        workbook = load_workbook(BytesIO(content), read_only=True, data_only=True)

        self.assertEqual(workbook.sheetnames, ["敏感词导入"])
        sheet = workbook["敏感词导入"]
        self.assertEqual(sheet["A1"].value, "敏感词")

    def test_import_counts_created_duplicate_and_invalid_rows(self) -> None:
        from openpyxl import Workbook

        from app.services import sensitive_word_service

        workbook = Workbook()
        sheet = workbook.active
        sheet.title = "敏感词导入"
        sheet.append(["敏感词"])
        sheet.append(["春物"])
        sheet.append(["即納"])
        sheet.append(["春物"])
        sheet.append(["   "])
        sheet.append(["翌日配達"])
        buffer = BytesIO()
        workbook.save(buffer)

        with patch.object(sensitive_word_service, "SessionLocal", self.session_factory):
            sensitive_word_service.create_sensitive_word("即納")
            result = sensitive_word_service.import_sensitive_words(
                content=buffer.getvalue(),
                filename="sensitive-words.xlsx",
            )

        self.assertEqual(
            result,
            {
                "createdCount": 2,
                "duplicateCount": 2,
                "invalidCount": 1,
            },
        )
        self.assertEqual(self.list_words(), ["即納", "春物", "翌日配達"])

    def test_import_rejects_non_xlsx_files(self) -> None:
        from app.services.sensitive_word_service import import_sensitive_words

        with self.assertRaisesRegex(RuntimeError, r"\.xlsx"):
            import_sensitive_words(content=b"not-an-excel-file", filename="sensitive-words.csv")

    def test_import_rejects_valid_zip_that_is_not_a_real_xlsx_workbook(self) -> None:
        from app.services.sensitive_word_service import import_sensitive_words

        buffer = BytesIO()
        with ZipFile(buffer, "w") as archive:
            archive.writestr("not-a-workbook.txt", "plain text")

        with self.assertRaisesRegex(RuntimeError, r"有效的 \.xlsx"):
            import_sensitive_words(content=buffer.getvalue(), filename="sensitive-words.xlsx")

    def test_import_requires_sensitive_word_header(self) -> None:
        from openpyxl import Workbook

        from app.services.sensitive_word_service import import_sensitive_words

        workbook = Workbook()
        sheet = workbook.active
        sheet.title = "敏感词导入"
        sheet.append(["词汇"])
        sheet.append(["春物"])
        buffer = BytesIO()
        workbook.save(buffer)

        with self.assertRaisesRegex(RuntimeError, "表头"):
            import_sensitive_words(content=buffer.getvalue(), filename="sensitive-words.xlsx")

    def test_import_counts_flush_time_uniqueness_race_as_duplicate_and_keeps_valid_rows(self) -> None:
        from openpyxl import Workbook

        from app.db.models import SensitiveWordModel
        from app.services import sensitive_word_service

        race_engine = None
        race_session_factory = None
        inject_competing_insert = None

        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = f"{temp_dir}\\sensitive-word-race.sqlite3"
            race_engine = create_engine(f"sqlite+pysqlite:///{database_path}", future=True)
            try:
                Base.metadata.create_all(race_engine)
                race_session_factory = sessionmaker(
                    bind=race_engine,
                    expire_on_commit=False,
                    future=True,
                )

                workbook = Workbook()
                sheet = workbook.active
                sheet.title = "敏感词导入"
                sheet.append(["敏感词"])
                sheet.append(["春物"])
                sheet.append(["競合語"])
                sheet.append(["翌日配達"])
                buffer = BytesIO()
                workbook.save(buffer)

                injected_words: list[str] = []

                @event.listens_for(race_session_factory.class_, "before_flush")
                def inject_competing_insert(session, flush_context, instances) -> None:
                    if not session.info.get("inject_race_once") or session.info.get("race_injected"):
                        return

                    pending_words = {
                        row.word
                        for row in session.new
                        if isinstance(row, SensitiveWordModel)
                    }
                    if "競合語" not in pending_words:
                        return

                    session.info["race_injected"] = True
                    with race_session_factory() as competing_session:
                        competing_session.add(SensitiveWordModel(word="競合語", enabled=True))
                        competing_session.commit()
                    injected_words.append("競合語")

                def flagged_session_local():
                    session = race_session_factory()
                    session.info["inject_race_once"] = True
                    return session

                with patch.object(sensitive_word_service, "SessionLocal", flagged_session_local):
                    result = sensitive_word_service.import_sensitive_words(
                        content=buffer.getvalue(),
                        filename="sensitive-words.xlsx",
                    )

                self.assertEqual(injected_words, ["競合語"])
                self.assertEqual(
                    result,
                    {
                        "createdCount": 2,
                        "duplicateCount": 1,
                        "invalidCount": 0,
                    },
                )

                with Session(race_engine, future=True) as session:
                    words = session.scalars(
                        select(SensitiveWordModel.word).order_by(SensitiveWordModel.id.asc())
                    ).all()

                self.assertEqual(words, ["春物", "競合語", "翌日配達"])
            finally:
                if race_session_factory is not None and inject_competing_insert is not None:
                    event.remove(race_session_factory.class_, "before_flush", inject_competing_insert)
                if race_engine is not None:
                    race_engine.dispose()


class SensitiveWordUpsertTests(SensitiveWordDatabaseTestCase):
    def test_upsert_product_sanitizes_stored_title_and_payload_with_active_words(self) -> None:
        from app.db.models import ProductModel, SensitiveWordModel
        from app.services import crawler_service

        item = {
            "source_url": "https://example.test/item/1",
            "title": "【楽天1位】 即納 春物",
            "raw": {
                "title": "【楽天1位】 即納 春物",
                "itemName": "【楽天1位】 即納 春物",
                "tagline": "翌日配達 おすすめ",
            },
        }

        with self.session_scope() as session:
            session.add_all(
                [
                    SensitiveWordModel(word="【】", enabled=True),
                    SensitiveWordModel(word="即納", enabled=True),
                    SensitiveWordModel(word="翌日配達", enabled=True),
                ]
            )
            session.flush()

            saved = crawler_service.upsert_product(session, "alice", "task-1", item)
            self.assertTrue(saved)
            session.flush()

            row = session.scalar(select(ProductModel))
            self.assertIsNotNone(row)
            assert row is not None
            self.assertEqual(row.title, "春物")
            self.assertEqual(
                json.loads(row.raw_payload_json),
                {
                    "title": "春物",
                    "itemName": "春物",
                    "tagline": "おすすめ",
                },
            )

        self.assertEqual(item["title"], "【楽天1位】 即納 春物")
        self.assertEqual(item["raw"]["title"], "【楽天1位】 即納 春物")
        self.assertEqual(item["raw"]["itemName"], "【楽天1位】 即納 春物")
        self.assertEqual(item["raw"]["tagline"], "翌日配達 おすすめ")

    def test_upsert_product_returns_false_when_sanitized_title_becomes_empty(self) -> None:
        from app.db.models import ProductModel, SensitiveWordModel
        from app.services import crawler_service

        item = {
            "source_url": "https://example.test/item/2",
            "title": "【楽天1位】 即納",
            "raw": {
                "title": "【楽天1位】 即納",
                "itemName": "【楽天1位】 即納",
            },
        }

        with self.session_scope() as session:
            session.add_all(
                [
                    SensitiveWordModel(word="【】", enabled=True),
                    SensitiveWordModel(word="即納", enabled=True),
                ]
            )
            session.flush()

            saved = crawler_service.upsert_product(session, "alice", "task-2", item)

            self.assertFalse(saved)
            self.assertEqual(session.scalar(select(func.count()).select_from(ProductModel)), 0)


if __name__ == "__main__":
    unittest.main()
