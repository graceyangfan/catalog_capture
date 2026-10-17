"""Offline tests for the Predict Testnet authentication probe."""

from __future__ import annotations

import unittest
from typing import Any

import requests
from eth_account import Account
from eth_account.messages import encode_defunct
from probe_predict_testnet_auth import (
    ProbeError,
    _erc20_call_data,
    _safe_error,
    _validate_testnet_api_url,
    authenticate_eoa,
    format_units,
)

TEST_PRIVATE_KEY = "0x" + "01".zfill(64)


class PredictTestnetAuthProbeTests(unittest.TestCase):
    def test_private_error_does_not_echo_venue_payload(self) -> None:
        response = requests.Response()
        response.status_code = 401
        response._content = b'{"code":"secret-token","message":"private-key", "error":"signature"}'
        self.assertEqual(
            str(_safe_error(response, "wallet authentication")),
            "wallet authentication failed with HTTP 401",
        )

    def test_authentication_signs_exact_challenge_and_keeps_fields_distinct(
        self,
    ) -> None:
        challenge = "Predict test challenge 123"
        calls: list[dict[str, Any]] = []

        def fake_request(
            session: requests.Session,
            method: str,
            url: str,
            **kwargs: Any,
        ) -> dict[str, Any]:
            del session
            calls.append({"method": method, "url": url, **kwargs})
            if method == "GET":
                return {"success": True, "data": {"message": challenge}}
            return {"success": True, "data": {"token": "test-jwt-not-printed"}}

        with requests.Session() as session:
            address, token = authenticate_eoa(
                session,
                "https://api-testnet.predict.fun",
                TEST_PRIVATE_KEY,
                "test-api-key",
                1.0,
                request=fake_request,
            )

        self.assertEqual(token, "test-jwt-not-printed")
        self.assertEqual(address, Account.from_key(TEST_PRIVATE_KEY).address)
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0]["headers"]["x-api-key"], "test-api-key")
        body = calls[1]["body"]
        self.assertEqual(body["signer"], address)
        self.assertEqual(body["message"], challenge)
        recovered = Account.recover_message(
            encode_defunct(text=challenge),
            signature=body["signature"],
        )
        self.assertEqual(recovered, address)

    def test_mainnet_url_is_rejected(self) -> None:
        with self.assertRaises(ProbeError):
            _validate_testnet_api_url("https://api.predict.fun")

    def test_erc20_balance_call_uses_full_padded_address(self) -> None:
        data = _erc20_call_data(
            "0x70a08231", "0x00000000000000000000000000000000000000aB"
        )
        self.assertEqual(len(data), 2 + 8 + 64)
        self.assertTrue(data.endswith("00000000000000000000000000000000000000ab"))

    def test_format_units_is_exact(self) -> None:
        self.assertEqual(format_units(1_234_500_000_000_000_000, 18), "1.2345")


if __name__ == "__main__":
    unittest.main()
