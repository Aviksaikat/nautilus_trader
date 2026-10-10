// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Configuration structures for the Derive adapter.

use std::time::Duration;

use anyhow::Context;
use nautilus_core::string::secret::SecretString;
use nautilus_model::identifiers::AccountId;
use nautilus_network::websocket::TransportBackend;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::common::{enums::DeriveEnvironment, urls};

/// Configuration for the Derive data client.
#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
#[serde(default, deny_unknown_fields)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.derive", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.derive")
)]
pub struct DeriveDataClientConfig {
    /// The Derive environment to connect to.
    #[builder(default)]
    pub environment: DeriveEnvironment,
    /// Override for the REST API base URL.
    pub base_url_rest: Option<String>,
    /// Override for the WebSocket URL.
    pub base_url_ws: Option<String>,
    /// Optional proxy URL for HTTP and WebSocket transports.
    pub proxy_url: Option<SecretString>,
    /// HTTP timeout in seconds.
    #[builder(default = 10)]
    pub http_timeout_secs: u64,
    /// Optional per-operation WebSocket timeout in seconds (login, subscribe,
    /// reads, writes). When unset, the low-level `WS_REQUEST_TIMEOUT` applies.
    pub ws_timeout_secs: Option<u64>,
    /// Interval for refreshing instruments in minutes.
    #[builder(default = 60)]
    pub update_instruments_interval_mins: u64,
    /// Underlying currencies to load on connect. Empty means lazy-load by
    /// instrument ID when subscribing.
    #[builder(default)]
    pub currencies: Vec<String>,
    /// Whether instrument loading includes expired instruments.
    #[builder(default)]
    pub include_expired: bool,
    /// Whether subscriptions may fetch missing instruments before sending the
    /// WebSocket request.
    #[builder(default = true)]
    pub auto_load_missing_instruments: bool,
    /// Whether REST polling may temporarily replace stale quote WebSocket data.
    #[builder(default)]
    pub rest_market_data_failover_enabled: bool,
    /// REST quote polling interval in milliseconds. Required when failover is enabled.
    pub rest_market_data_poll_interval_ms: Option<u64>,
    /// Maximum accepted REST quote age in seconds. Required when failover is enabled.
    pub rest_market_data_max_quote_age_secs: Option<u64>,
    /// WebSocket transport backend (defaults to `Sockudo` when that feature is enabled).
    #[builder(default)]
    pub transport_backend: TransportBackend,
}

#[cfg(feature = "python")]
nautilus_core::impl_pyo3_config_getters!(DeriveDataClientConfig {
    environment: DeriveEnvironment,
    base_url_rest: Option<String>,
    base_url_ws: Option<String>,
    http_timeout_secs: u64,
    ws_timeout_secs: Option<u64>,
    update_instruments_interval_mins: u64,
    currencies: Vec<String>,
    include_expired: bool,
    auto_load_missing_instruments: bool,
    rest_market_data_failover_enabled: bool,
    rest_market_data_poll_interval_ms: Option<u64>,
    rest_market_data_max_quote_age_secs: Option<u64>,
    transport_backend: TransportBackend,
});

impl Default for DeriveDataClientConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl DeriveDataClientConfig {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the REST API base URL, respecting environment and overrides.
    #[must_use]
    pub fn rest_url(&self) -> String {
        self.base_url_rest
            .clone()
            .unwrap_or_else(|| urls::rest_url(self.environment).to_string())
    }

    /// Returns the WebSocket URL, respecting environment and overrides.
    #[must_use]
    pub fn ws_url(&self) -> String {
        self.base_url_ws
            .clone()
            .unwrap_or_else(|| urls::ws_url(self.environment).to_string())
    }

    /// Validates data configuration invariants.
    ///
    /// # Errors
    ///
    /// Returns an error when REST market-data failover is enabled without a
    /// positive polling interval and maximum quote age.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.rest_market_data_failover_enabled {
            self.rest_market_data_poll_interval_ms
                .filter(|interval| *interval > 0)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "REST market-data failover requires a non-zero polling interval"
                    )
                })?;

            let max_quote_age_secs = self
                .rest_market_data_max_quote_age_secs
                .filter(|age| *age > 0)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "REST market-data failover requires a non-zero maximum quote age"
                    )
                })?;
            u64::try_from(Duration::from_secs(max_quote_age_secs).as_nanos())
                .context("maximum REST quote age exceeds the supported duration")?;
        }
        Ok(())
    }
}

/// Configuration for the Derive execution client.
#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
#[serde(default, deny_unknown_fields)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.derive", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.derive")
)]
pub struct DeriveExecutionClientConfig {
    /// Account identifier for the execution client.
    #[builder(default = AccountId::from("DERIVE-001"))]
    pub account_id: AccountId,
    /// Derive Chain smart-contract wallet address (`X-LYRAWALLET`). Falls back
    /// to `DERIVE_WALLET_ADDRESS` (or `DERIVE_TESTNET_WALLET_ADDRESS` on
    /// testnet) when unset.
    pub wallet_address: Option<String>,
    /// secp256k1 session-key private key in hex (with or without `0x` prefix).
    /// Falls back to `DERIVE_SESSION_PRIVATE_KEY` (or
    /// `DERIVE_TESTNET_SESSION_PRIVATE_KEY` on testnet) when unset.
    pub session_key: Option<SecretString>,
    /// Subaccount identifier. Falls back to `DERIVE_SUBACCOUNT_ID` (or
    /// `DERIVE_TESTNET_SUBACCOUNT_ID` on testnet) when unset.
    pub subaccount_id: Option<u64>,
    /// The Derive environment to connect to.
    #[builder(default)]
    pub environment: DeriveEnvironment,
    /// Override for the REST API base URL.
    pub base_url_rest: Option<String>,
    /// Override for the WebSocket URL.
    pub base_url_ws: Option<String>,
    /// Optional proxy URL for HTTP and WebSocket transports.
    pub proxy_url: Option<SecretString>,
    /// HTTP timeout in seconds.
    #[builder(default = 10)]
    pub http_timeout_secs: u64,
    /// Maximum number of retry attempts for HTTP requests.
    #[builder(default = 3)]
    pub max_retries: u32,
    /// Initial retry delay in milliseconds.
    #[builder(default = 100)]
    pub retry_delay_initial_ms: u64,
    /// Maximum retry delay in milliseconds.
    #[builder(default = 5000)]
    pub retry_delay_max_ms: u64,
    /// Optional per-operation WebSocket timeout in seconds (login, subscribe,
    /// reads, writes). When unset, the low-level `WS_REQUEST_TIMEOUT` applies.
    pub ws_timeout_secs: Option<u64>,
    /// Per-contract USDC fee cap signed into every order. Required for
    /// execution and must be greater than zero.
    pub max_fee_per_contract: Option<Decimal>,
    /// WebSocket transport backend (defaults to `Sockudo` when that feature is enabled).
    #[builder(default)]
    pub transport_backend: TransportBackend,
    /// Override for the EIP-712 domain separator. Falls back to the constant
    /// for the configured environment when unset. The shipped constants are
    /// placeholders that must be replaced or overridden before signing.
    pub domain_separator: Option<String>,
    /// Override for the EIP-712 action typehash. Falls back to the shipped
    /// [`crate::common::consts::ACTION_TYPEHASH`] when unset.
    pub action_typehash: Option<String>,
    /// Override for the Trade module contract address. Falls back to the
    /// shipped per-environment constant when unset.
    pub trade_module_address: Option<String>,
    /// Signature expiry TTL in seconds for normal orders and replaces (added
    /// to the wall clock before signing). Must be greater than the venue
    /// minimum ([`crate::common::consts::MIN_SIGNATURE_TTL`], 300s).
    #[builder(default = 600)]
    pub signature_expiry_secs: u64,
    /// Slippage bound applied to market orders when deriving a worst-acceptable
    /// limit price from the cached top-of-book quote. Expressed in basis points
    /// (1 bp = 0.01%). Defaults to 50 bp = 0.5%.
    #[builder(default = 50)]
    pub market_order_slippage_bps: u32,
    /// Maximum matching-engine requests per second for order writes sent over
    /// the WebSocket (create/cancel/replace). Defaults to the Trader-tier limit
    /// of 1 when unset; raise it for Market Maker accounts with higher
    /// negotiated limits. See <https://docs.derive.xyz/reference/rate-limits>.
    pub max_matching_requests_per_second: Option<u32>,
    /// Maximum per-instrument matching requests per second for instrument-
    /// scoped order writes sent over the WebSocket. Defaults to the Trader-tier
    /// limit of 1 when unset; raise it for Market Maker accounts with higher
    /// negotiated per-instrument limits. This allowance is independent of
    /// `max_matching_requests_per_second`, which never inflates it. See
    /// <https://docs.derive.xyz/reference/rate-limits>.
    pub max_per_instrument_matching_requests_per_second: Option<u32>,
    /// Enables signed REST order writes and private-state polling while the
    /// execution WebSocket is unavailable or unauthenticated.
    #[builder(default)]
    pub rest_fallback_enabled: bool,
    /// Interval in seconds between private-state reconciliations while REST
    /// fallback is active. Required when `rest_fallback_enabled` is true.
    pub private_state_poll_interval_secs: Option<u64>,
    /// Maximum age in seconds of the last successful private-state
    /// reconciliation for allowing risk-increasing orders. Required when
    /// `rest_fallback_enabled` is true.
    pub max_account_state_age_secs: Option<u64>,
    /// Maximum age in seconds of the quote used for market-order pricing and
    /// the risk-increasing entry/replace gate while REST fallback is active.
    /// Independent of the data client's `rest_market_data_max_quote_age_secs`
    /// — execution must honor its own bound rather than assume data's is at
    /// least as strict. Required when `rest_fallback_enabled` is true.
    pub max_quote_age_secs: Option<u64>,
}

#[cfg(feature = "python")]
nautilus_core::impl_pyo3_config_getters!(DeriveExecutionClientConfig {
    account_id: AccountId,
    wallet_address: Option<String>,
    subaccount_id: Option<u64>,
    environment: DeriveEnvironment,
    base_url_rest: Option<String>,
    base_url_ws: Option<String>,
    http_timeout_secs: u64,
    max_retries: u32,
    retry_delay_initial_ms: u64,
    retry_delay_max_ms: u64,
    ws_timeout_secs: Option<u64>,
    max_fee_per_contract: Option<Decimal>,
    domain_separator: Option<String>,
    action_typehash: Option<String>,
    trade_module_address: Option<String>,
    signature_expiry_secs: u64,
    market_order_slippage_bps: u32,
    max_matching_requests_per_second: Option<u32>,
    max_per_instrument_matching_requests_per_second: Option<u32>,
    rest_fallback_enabled: bool,
    private_state_poll_interval_secs: Option<u64>,
    max_account_state_age_secs: Option<u64>,
    max_quote_age_secs: Option<u64>,
    transport_backend: TransportBackend,
});

impl Default for DeriveExecutionClientConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl DeriveExecutionClientConfig {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true when wallet, session-key, and subaccount are all populated
    /// **in this config**. Environment-variable fallbacks documented on the
    /// individual fields are resolved at factory-construction time, not here;
    /// callers that need a "credentials available anywhere" check should
    /// inspect both this method and the relevant env vars.
    #[must_use]
    pub fn has_credentials(&self) -> bool {
        self.wallet_address
            .as_deref()
            .is_some_and(|s| !s.trim().is_empty())
            && self
                .session_key
                .as_ref()
                .map(SecretString::expose_secret)
                .is_some_and(|s| !s.trim().is_empty())
            && self.subaccount_id.is_some()
    }

    /// Validates execution configuration invariants.
    ///
    /// # Errors
    ///
    /// Returns an error when `max_fee_per_contract` is missing or not greater
    /// than zero.
    pub fn validate(&self) -> anyhow::Result<()> {
        let Some(max_fee_per_contract) = self.max_fee_per_contract else {
            anyhow::bail!("max_fee_per_contract is required");
        };

        if max_fee_per_contract <= Decimal::ZERO {
            anyhow::bail!("max_fee_per_contract must be greater than zero");
        }

        if self.rest_fallback_enabled {
            let Some(interval) = self.private_state_poll_interval_secs else {
                anyhow::bail!(
                    "private_state_poll_interval_secs is required when REST fallback is enabled"
                );
            };
            if interval == 0 {
                anyhow::bail!("private_state_poll_interval_secs must be greater than zero");
            }

            let Some(max_age) = self.max_account_state_age_secs else {
                anyhow::bail!(
                    "max_account_state_age_secs is required when REST fallback is enabled"
                );
            };
            if max_age == 0 {
                anyhow::bail!("max_account_state_age_secs must be greater than zero");
            }

            let Some(max_quote_age) = self.max_quote_age_secs else {
                anyhow::bail!("max_quote_age_secs is required when REST fallback is enabled");
            };
            if max_quote_age == 0 {
                anyhow::bail!("max_quote_age_secs must be greater than zero");
            }
        }
        Ok(())
    }

    /// Returns the REST API base URL, respecting environment and overrides.
    #[must_use]
    pub fn rest_url(&self) -> String {
        self.base_url_rest
            .clone()
            .unwrap_or_else(|| urls::rest_url(self.environment).to_string())
    }

    /// Returns the WebSocket URL, respecting environment and overrides.
    #[must_use]
    pub fn ws_url(&self) -> String {
        self.base_url_ws
            .clone()
            .unwrap_or_else(|| urls::ws_url(self.environment).to_string())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_data_config_defaults() {
        let config = DeriveDataClientConfig::default();
        assert_eq!(config.environment, DeriveEnvironment::Mainnet);
        assert_eq!(config.http_timeout_secs, 10);
        assert_eq!(config.ws_timeout_secs, None);
        assert_eq!(config.update_instruments_interval_mins, 60);
        assert!(config.currencies.is_empty());
        assert!(!config.include_expired);
        assert!(config.auto_load_missing_instruments);
        assert!(!config.rest_market_data_failover_enabled);
        assert!(config.rest_market_data_poll_interval_ms.is_none());
        assert!(config.rest_market_data_max_quote_age_secs.is_none());
    }

    #[rstest]
    fn test_data_config_validate_accepts_disabled_default() {
        assert!(DeriveDataClientConfig::default().validate().is_ok());
    }

    #[rstest]
    fn test_data_config_validate_requires_bounds_when_failover_enabled() {
        let base = DeriveDataClientConfig {
            rest_market_data_failover_enabled: true,
            rest_market_data_poll_interval_ms: Some(20_000),
            rest_market_data_max_quote_age_secs: Some(60),
            ..DeriveDataClientConfig::default()
        };
        assert!(base.validate().is_ok());

        let missing_interval = DeriveDataClientConfig {
            rest_market_data_poll_interval_ms: None,
            ..base.clone()
        };
        assert!(
            missing_interval
                .validate()
                .unwrap_err()
                .to_string()
                .contains("non-zero polling interval"),
        );

        let zero_age = DeriveDataClientConfig {
            rest_market_data_max_quote_age_secs: Some(0),
            ..base
        };
        assert!(
            zero_age
                .validate()
                .unwrap_err()
                .to_string()
                .contains("non-zero maximum quote age"),
        );
    }

    #[rstest]
    fn test_data_config_urls_mainnet() {
        let config = DeriveDataClientConfig::default();
        assert!(config.rest_url().contains("api.lyra.finance"));
        assert!(config.ws_url().contains("api.lyra.finance"));
    }

    #[rstest]
    fn test_data_config_urls_testnet() {
        let config = DeriveDataClientConfig {
            environment: DeriveEnvironment::Testnet,
            ..DeriveDataClientConfig::default()
        };
        assert!(config.rest_url().contains("demo"));
        assert!(config.ws_url().contains("demo"));
    }

    #[rstest]
    fn test_exec_config_defaults() {
        let config = DeriveExecutionClientConfig::default();
        assert_eq!(config.environment, DeriveEnvironment::Mainnet);
        assert_eq!(config.http_timeout_secs, 10);
        assert_eq!(config.max_retries, 3);
        assert!(config.max_matching_requests_per_second.is_none());
        assert!(
            config
                .max_per_instrument_matching_requests_per_second
                .is_none()
        );
        assert!(!config.rest_fallback_enabled);
        assert!(config.private_state_poll_interval_secs.is_none());
        assert!(config.max_account_state_age_secs.is_none());
        assert!(config.max_quote_age_secs.is_none());
        assert!(!config.has_credentials());
    }

    #[rstest]
    fn test_exec_config_validate_requires_max_quote_age_secs_when_rest_fallback_enabled() {
        let base = DeriveExecutionClientConfig {
            max_fee_per_contract: Some(Decimal::ONE),
            rest_fallback_enabled: true,
            private_state_poll_interval_secs: Some(30),
            max_account_state_age_secs: Some(90),
            ..DeriveExecutionClientConfig::default()
        };

        let missing = DeriveExecutionClientConfig {
            max_quote_age_secs: None,
            ..base.clone()
        };
        assert!(
            missing
                .validate()
                .unwrap_err()
                .to_string()
                .contains("max_quote_age_secs is required"),
        );

        let zero = DeriveExecutionClientConfig {
            max_quote_age_secs: Some(0),
            ..base.clone()
        };
        assert!(
            zero.validate()
                .unwrap_err()
                .to_string()
                .contains("max_quote_age_secs must be greater than zero"),
        );

        let valid = DeriveExecutionClientConfig {
            max_quote_age_secs: Some(60),
            ..base
        };
        assert!(valid.validate().is_ok());
    }

    #[rstest]
    fn test_exec_config_has_credentials_requires_all_three_fields() {
        let mut config = DeriveExecutionClientConfig {
            wallet_address: Some("0x1234".to_string()),
            ..DeriveExecutionClientConfig::default()
        };
        assert!(!config.has_credentials());

        config.session_key = Some("0xabcd".into());
        assert!(!config.has_credentials());

        config.subaccount_id = Some(1);
        assert!(config.has_credentials());
    }

    #[rstest]
    fn test_exec_config_has_credentials_rejects_blank_strings() {
        let config = DeriveExecutionClientConfig {
            wallet_address: Some("   ".to_string()),
            session_key: Some("0xabcd".into()),
            subaccount_id: Some(1),
            ..DeriveExecutionClientConfig::default()
        };
        assert!(!config.has_credentials());
    }

    #[rstest]
    fn test_exec_config_debug_redacts_session_key() {
        // Use a low-entropy sentinel rather than a hex private key so the
        // assertion exercises Debug-redaction without tripping the secrets
        // scanner on a synthetic test value. The redaction logic is
        // string-content-agnostic.
        let session_key = "FAKE_SESSION_KEY_SENTINEL";
        let config = DeriveExecutionClientConfig {
            wallet_address: Some("0xWALLET".to_string()),
            session_key: Some(session_key.into()),
            subaccount_id: Some(42),
            ..DeriveExecutionClientConfig::default()
        };
        let debug = format!("{config:?}");
        assert!(debug.contains("redacted"));
        assert!(!debug.contains(session_key));
        assert!(debug.contains("0xWALLET"));
        assert!(debug.contains("42"));
    }

    #[rstest]
    fn test_exec_config_debug_omits_session_key_marker_when_unset() {
        let config = DeriveExecutionClientConfig::default();
        let debug = format!("{config:?}");
        assert!(!debug.contains("redacted"));
        assert!(debug.contains("session_key: None"));
    }
}
