/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use carbide_secrets::credentials::Credentials;
use carbide_utils::HostPortPair;
pub use nv_redfish::bmc_http::reqwest::BmcError;
use nv_redfish::bmc_http::reqwest::{
    Client as RedfishReqwestClient, ClientParams as RedfishReqwestClientParams,
};
use nv_redfish::bmc_http::{BmcCredentials, CacheSettings, HttpBmc};
use nv_redfish::oem::hpe::ilo_service_ext::ManagerType as HpeManagerType;
use nv_redfish::{Error as NvError, ServiceRoot as NvServiceRoot};
use reqwest::header::HeaderMap;
use url::Url;

pub type RedfishBmc = HttpBmc<RedfishReqwestClient>;
pub type ServiceRoot = NvServiceRoot<RedfishBmc>;
pub type Error = NvError<RedfishBmc>;

pub fn new_pool(proxy_address: Arc<ArcSwap<Option<HostPortPair>>>) -> Arc<NvRedfishClientPool> {
    NvRedfishClientPool::new(proxy_address).into()
}

pub struct NvRedfishClientPool {
    proxy_address: Arc<ArcSwap<Option<HostPortPair>>>,
    cache: Arc<Mutex<HashMap<PoolKey, Arc<ServiceRoot>>>>,
}

#[derive(Hash, PartialEq, Eq)]
struct PoolKey {
    proxy_address: Arc<Option<HostPortPair>>,
    bmc_address: SocketAddr,
    credentials: BmcCredentials,
}

impl NvRedfishClientPool {
    pub fn new(proxy_address: Arc<ArcSwap<Option<HostPortPair>>>) -> Self {
        Self {
            proxy_address,
            cache: Default::default(),
        }
    }

    pub async fn service_root(
        &self,
        bmc_address: SocketAddr,
        credentials: Credentials,
    ) -> Result<Arc<ServiceRoot>, Error> {
        self.service_root_with_cache_predicate(bmc_address, credentials, |_| true)
            .await
    }

    /// Same as [`Self::service_root`], but a freshly fetched root is cached
    /// only when `should_cache` returns true for it.
    pub async fn service_root_with_cache_predicate(
        &self,
        bmc_address: SocketAddr,
        credentials: Credentials,
        should_cache: impl FnOnce(&ServiceRoot) -> bool,
    ) -> Result<Arc<ServiceRoot>, Error> {
        let Credentials::UsernamePassword { username, password } = credentials;
        let bmc_credentials = BmcCredentials::new(username, password);

        if let Some(sevice_root) = self.cached_root(bmc_address, bmc_credentials.clone()) {
            Ok(sevice_root)
        } else {
            let bmc = self.create_bmc(bmc_address, bmc_credentials.clone(), false)?;
            let service_root = ServiceRoot::new(bmc).await?;
            let service_root = if service_root.vendor()
                == Some(nv_redfish::service_root::Vendor::new("HPE"))
                && let Some(HpeManagerType::Ilo(version)) = service_root
                    .oem_hpe_ilo_service_ext()
                    .ok()
                    .as_ref()
                    .and_then(|v| v.as_ref())
                    .and_then(|v| v.manager_type())
                && version < 7
            {
                // Handle HPE BMC that closing connection right after
                // response. In this case, we add Connection: Close
                // HTTP header to prevent trying to reuse this
                // connection. Otherwise, race condition may happen
                // when reqwest thinks that connection is alive but it
                // is about to close by server. Reusing such
                // connections causes errors.
                let bmc = self.create_bmc(bmc_address, bmc_credentials.clone(), true)?;
                service_root.replace_bmc(bmc.clone())
            } else {
                service_root
            };
            let service_root = Arc::new(service_root);
            if should_cache(&service_root) {
                self.update_cache(bmc_address, bmc_credentials, service_root.clone());
            }
            Ok(service_root)
        }
    }

    fn cached_root(
        &self,
        bmc_address: SocketAddr,
        credentials: BmcCredentials,
    ) -> Option<Arc<ServiceRoot>> {
        let proxy_address = self.proxy_address.load();
        let key = PoolKey {
            proxy_address: proxy_address.clone(),
            bmc_address,
            credentials,
        };
        self.cache
            .lock()
            .expect("nv-redish client cache mutex poisoned")
            .get(&key)
            .cloned()
    }

    fn update_cache(
        &self,
        bmc_address: SocketAddr,
        credentials: BmcCredentials,
        root: Arc<ServiceRoot>,
    ) {
        let proxy_address = self.proxy_address.load();
        let key = PoolKey {
            proxy_address: proxy_address.clone(),
            bmc_address,
            credentials,
        };
        let mut cache = self
            .cache
            .lock()
            .expect("nv-redish client cache mutex poisoned");
        cache.insert(key, root);
    }

    pub fn create_bmc(
        &self,
        bmc_address: SocketAddr,
        credentials: BmcCredentials,
        connection_close: bool,
    ) -> Result<Arc<RedfishBmc>, Error> {
        let proxy_address = self.proxy_address.load();
        let bmc_url = build_bmc_url(proxy_address.as_ref(), bmc_address);

        let mut headers = HeaderMap::new();
        if proxy_address.is_some() {
            headers.insert(
                reqwest::header::FORWARDED,
                format!("host={}", bmc_address.ip())
                    .parse()
                    .expect("Generated header is expected to be valid"),
            );
        }
        if connection_close {
            headers.insert(
                reqwest::header::CONNECTION,
                reqwest::header::HeaderValue::from_static("Close"),
            );
        }

        let client = RedfishReqwestClient::with_params(
            RedfishReqwestClientParams::new().accept_invalid_certs(true),
        )
        .map_err(|err| Error::Bmc(err.into()))?;
        Ok(Arc::new(RedfishBmc::with_custom_headers(
            client,
            bmc_url,
            credentials,
            CacheSettings::with_capacity(10),
            headers,
        )))
    }
}

/// Builds the BMC base URL, applying any configured proxy override.
///
/// The `url` crate assembles the authority: `set_ip_host` brackets IPv6 literals
/// for us, so an IPv6 BMC (or proxy host) yields a valid URL. A bare, unbracketed
/// `2001:db8::1` authority would otherwise be rejected by the parser.
fn build_bmc_url(proxy_address: &Option<HostPortPair>, bmc_address: SocketAddr) -> Url {
    let mut url = Url::parse("https://placeholder.invalid").expect("static base URL is valid");
    let port = match proxy_address {
        None => {
            set_ip_host(&mut url, bmc_address.ip());
            bmc_address.port()
        }
        Some(HostPortPair::HostAndPort(h, p)) => {
            set_host(&mut url, h);
            *p
        }
        Some(HostPortPair::HostOnly(h)) => {
            set_host(&mut url, h);
            bmc_address.port()
        }
        Some(HostPortPair::PortOnly(p)) => {
            set_ip_host(&mut url, bmc_address.ip());
            *p
        }
    };
    url.set_port(Some(port))
        .expect("https base URL accepts a port");
    url
}

/// Sets an IP host, letting the `url` crate bracket IPv6 literals.
fn set_ip_host(url: &mut Url, ip: IpAddr) {
    url.set_ip_host(ip)
        .expect("https base URL accepts an IP host");
}

/// Sets a proxy host that may be a hostname or an IP literal. IP literals go
/// through `set_ip_host` (which brackets IPv6); everything else through
/// `set_host`.
fn set_host(url: &mut Url, host: &str) {
    match host.parse::<IpAddr>() {
        Ok(ip) => set_ip_host(url, ip),
        Err(_) => url
            .set_host(Some(host))
            .expect("proxy host is expected to be a valid URL host"),
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use carbide_utils::HostPortPair;

    use super::build_bmc_url;

    fn sock(s: &str) -> SocketAddr {
        s.parse().expect("valid socket addr")
    }

    // Regression: an IPv6 BMC behind a port-only proxy must yield a bracketed
    // authority. Pre-fix the manual format produced `https://2001:db8::1:8443`,
    // which `Url::parse` rejects — and `create_bmc` `.expect()`s the parse, so it
    // panicked.
    #[test]
    fn port_only_proxy_brackets_ipv6_bmc() {
        let url = build_bmc_url(
            &Some(HostPortPair::PortOnly(8443)),
            sock("[2001:db8::1]:443"),
        );
        assert_eq!(url.host_str(), Some("[2001:db8::1]"));
        assert_eq!(url.port(), Some(8443));
        assert_eq!(url.as_str(), "https://[2001:db8::1]:8443/");
    }

    // IPv4 BMCs keep their unbracketed authority.
    #[test]
    fn port_only_proxy_leaves_ipv4_unchanged() {
        let url = build_bmc_url(&Some(HostPortPair::PortOnly(8443)), sock("10.0.0.5:443"));
        assert_eq!(url.host_str(), Some("10.0.0.5"));
        assert_eq!(url.port(), Some(8443));
    }

    // No proxy: the BMC's own IP and port form the authority; IPv6 is bracketed.
    // 443 is the https default, so the url crate canonicalizes it out of the
    // explicit port (as it always did when the old string was parsed).
    #[test]
    fn no_proxy_brackets_ipv6_bmc() {
        let url = build_bmc_url(&None, sock("[2001:db8::1]:443"));
        assert_eq!(url.host_str(), Some("[2001:db8::1]"));
        assert_eq!(url.port_or_known_default(), Some(443));
        assert_eq!(url.as_str(), "https://[2001:db8::1]/");
    }

    // A proxy host supplied as a bare IPv6 literal is bracketed too.
    #[test]
    fn proxy_host_ipv6_literal_is_bracketed() {
        let host_only = build_bmc_url(
            &Some(HostPortPair::HostOnly("2001:db8::2".to_string())),
            sock("10.0.0.5:443"),
        );
        assert_eq!(host_only.host_str(), Some("[2001:db8::2]"));
        assert_eq!(host_only.port_or_known_default(), Some(443));

        let host_and_port = build_bmc_url(
            &Some(HostPortPair::HostAndPort("2001:db8::2".to_string(), 8443)),
            sock("10.0.0.5:443"),
        );
        assert_eq!(host_and_port.host_str(), Some("[2001:db8::2]"));
        assert_eq!(host_and_port.port(), Some(8443));
    }

    // A hostname proxy is passed through untouched.
    #[test]
    fn proxy_hostname_unchanged() {
        let url = build_bmc_url(
            &Some(HostPortPair::HostAndPort(
                "bmc-proxy.example".to_string(),
                8443,
            )),
            sock("10.0.0.5:443"),
        );
        assert_eq!(url.host_str(), Some("bmc-proxy.example"));
        assert_eq!(url.port(), Some(8443));
    }
}
