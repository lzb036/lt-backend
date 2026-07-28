from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from app.services import crawler_service


def invalid_target_attribute_error() -> str:
    return json.dumps(
        {
            "errors": [
                {
                    "code": "IE0418",
                    "message": "Invalid attribute or genreId is set.",
                    "metadata": {
                        "details": [
                            {
                                "code": "invalidSelectiveValue",
                                "message": "The attributes.values is not defined as dictionary value.",
                                "properties": {"attributeName": "対象"},
                            }
                        ],
                        "propertyPath": "variants.r-sku00000003.attributes[1]",
                    },
                }
            ]
        },
        ensure_ascii=False,
    )


def payload_with_target_attributes(variant_count: int = 25) -> dict:
    return {
        "genreId": "566733",
        "variants": {
            f"r-sku{index:08d}": {
                "attributes": [
                    {"name": "カラー", "values": ["ブルー"]},
                    {"name": "対象", "values": ["女の子（キッズ）"]},
                ]
            }
            for index in range(1, variant_count + 1)
        },
    }


class RakutenInvalidSelectiveAttributeRetryTests(unittest.TestCase):
    def test_optional_invalid_attribute_is_removed_from_every_variant(self) -> None:
        payload = payload_with_target_attributes()

        with patch.object(
            crawler_service,
            "rakuten_attribute_rule_map_for_payload",
            return_value={"対象": {"required": False}},
        ):
            patched = crawler_service.patch_payload_for_invalid_selective_attribute_values(
                payload,
                invalid_target_attribute_error(),
            )

        self.assertIsNot(patched, payload)
        self.assertTrue(
            all(
                variant["attributes"] == [{"name": "カラー", "values": ["ブルー"]}]
                for variant in patched["variants"].values()
            )
        )
        self.assertTrue(
            all(
                any(attribute["name"] == "対象" for attribute in variant["attributes"])
                for variant in payload["variants"].values()
            )
        )

    def test_retry_submits_bulk_cleaned_payload_on_second_attempt(self) -> None:
        payload = payload_with_target_attributes()
        submitted_payloads: list[dict] = []

        def put_item(_secret: str, _key: str, _manage_number: str, submitted: dict) -> None:
            submitted_payloads.append(submitted)
            if len(submitted_payloads) == 1:
                raise RuntimeError(invalid_target_attribute_error())

        with (
            patch.object(crawler_service, "put_rakuten_item", side_effect=put_item),
            patch.object(
                crawler_service,
                "rakuten_attribute_rule_map_for_payload",
                return_value={"対象": {"required": False}},
            ),
        ):
            result = crawler_service.put_rakuten_item_with_attribute_retry(
                "secret",
                "key",
                "manage-number",
                payload,
            )

        self.assertEqual(len(submitted_payloads), 2)
        self.assertIs(result, submitted_payloads[1])
        self.assertTrue(
            all(
                all(attribute["name"] != "対象" for attribute in variant["attributes"])
                for variant in result["variants"].values()
            )
        )


if __name__ == "__main__":
    unittest.main()
