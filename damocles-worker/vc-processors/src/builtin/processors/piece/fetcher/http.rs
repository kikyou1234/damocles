use std::{env, time::Duration};
use std::error::Error;
use serde_urlencoded;
use anyhow::{anyhow, Context};
use lazy_static::lazy_static;
use reqwest::{
    blocking::{Client, ClientBuilder, Response},
    header, redirect, IntoUrl,
};

use super::PieceFetcher;

/// Returns the static reference to the `PieceHttpFetcher`
pub fn fetcher_ref() -> &'static PieceHttpFetcher {
    &PIECE_HTTP_FETCHER
}

lazy_static! {
    static ref PIECE_HTTP_FETCHER: PieceHttpFetcher =
        PieceHttpFetcher::from_env().unwrap();
}

/// A piece fetcher for the http file
pub struct PieceHttpFetcher {
    client: Client,
    redirect_client: Client,
    token: Option<String>,
}

impl<U: IntoUrl> PieceFetcher<U> for PieceHttpFetcher {
    type Err = anyhow::Error;
    type Read = Response;

    fn open(&self, u: U) -> Result<Self::Read, Self::Err> {


        let remote_file_url = u.as_str();
        match get_host_and_file(remote_file_url) {
            Ok((host, file)) => {
                
                FetchFileUrl = "/api/file_opt/fetch";

                let fetch_url = Url::parse(&host)
                    .map_err(|e| {
                        error!("[BH] parse url failed url: {}, err: {}", self.host, e);
                        e
                    })?;
        
                // 加载 FetchFileUrl（假设它是一个相对 URL 路径）
                let fetch_url = fetch_url.join(&FetchFileUrl).map_err(|e| {
                    error!("[BH] parse url failed url: {}, err: {}", "fetchFileUrl", e);
                    e
                })?;

                // 创建 URL 查询参数
                let mut params = vec![
                    ("file", file.clone()),
                    ("offer_confirmation", &"true"),
                ];

                // 将查询参数添加到 URL 中
                let fetch_url = fetch_url.join(&format!("?{}", serde_urlencoded::to_string(&params)?))
                    .map_err(|e| {
                        error!("[BH] add query params failed url: {}, err: {}", fetch_url, e);
                        e
                    })?;

                // 创建一个 HTTP 客户端
                let client = Client::builder()
                    .timeout(Duration::from_secs(24 * 3600))
                    .build()?;

                // 创建 HTTP 请求
                let request = client.request(Method::GET, fetch_url.clone())
                    .header("Connection", "close")
                    .build()
                    .map_err(|e| {
                        error!("[BH] NewRequest failed err: {}", e);
                        e
                    })?;

                // 发送请求并获取响应
                let resp = client.execute(request).await.map_err(|e| {
                    error!("[BH] request http failed err: {}", e);
                    e
                })?;

                if !resp.status().is_success(){
                    let status = resp.status();
                    let fetch_url = fetch_url.clone();
                    let body = resp.text().await.unwrap_or_else(|_| "Error reading body".to_string());
                    let err_msg = format!(
                        "[BH] {}:{} access: {}, body: {}",
                        status,
                        status.as_u16(),
                        fetch_url,
                        body
                    );
                    error!("{}", err_msg);
                    return Err(anyhow!(
                        "get resource {} failed invalid status code {}",
                        resp.url(),
                        status
                    ));
                }

                Ok(resp)

            }
            Err(e) => {
                return Err(anyhow!(
                    "remote url {} failed",
                    remote_file_url,
                ));
            }
        }

        // let u = u.into_url()?;
        // let mut resp = self
        //     .client
        //     .get(u.clone())
        //     .send()
        //     .context("request piece url")?;

        // let mut status_code = resp.status();
        // if status_code.is_redirection() {
        //     let redirect_url = resp
        //         .headers()
        //         .get(header::LOCATION)
        //         .context("redirect location not found")
        //         .and_then(|val| {
        //             val.to_str().context("convert redirect location to str")
        //         })
        //         .and_then(|location| {
        //             u.join(location).context("join redirect url")
        //         })?;

        //     let mut req = self.redirect_client.get(redirect_url);
        //     if let Some(token) = self.token.as_ref() {
        //         req = req
        //             .header(
        //                 header::AUTHORIZATION,
        //                 format!(
        //                     "{} {}",
        //                     Self::HEADER_AUTHORIZATION_BEARER_PREFIX,
        //                     token
        //                 ),
        //             )
        //             .header("X-VENUS-API-NAMESPACE", "v1.IMarket")
        //     };
        //     resp = req.send().context("request to redirected location")?;
        //     status_code = resp.status();
        // }

        // if !status_code.is_success() {
        //     return Err(anyhow!(
        //         "get resource {} failed invalid status code {}",
        //         resp.url(),
        //         status_code
        //     ));
        // }

        // Ok(resp)
    }
}

impl PieceHttpFetcher {
    pub const HEADER_AUTHORIZATION_BEARER_PREFIX: &'static str = "Bearer";
    pub const ENV_KEY_PIECE_FETCHER_TOKEN: &'static str = "PIECE_FETCHER_TOKEN";

    fn from_env() -> anyhow::Result<Self> {
        let token = env::var(Self::ENV_KEY_PIECE_FETCHER_TOKEN).ok();
        Self::new(token)
    }

    fn new(token: Option<String>) -> anyhow::Result<Self> {
        fn build_http_client(
            policy: redirect::Policy,
        ) -> reqwest::Result<Client> {
            ClientBuilder::new()
                .redirect(policy) // handle redirect ourselves
                .tcp_keepalive(Duration::from_secs(120))
                .connect_timeout(Duration::from_secs(5))
                .connection_verbose(true)
                .pool_max_idle_per_host(10)
                .build()
        }

        let client = build_http_client(redirect::Policy::none())
            .context("build http client")?;
        let redirect_client = build_http_client(redirect::Policy::default())
            .context("build redirect http client")?;
        Ok(Self {
            client,
            redirect_client,
            token,
        })
    }
}


#[derive(Debug)]
pub struct InvalidRemoteFileUrl {
    pub message: String,
}

impl fmt::Display for InvalidRemoteFileUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InvalidRemoteFileUrl: {}", self.message)
    }
}

impl Error for InvalidRemoteFileUrl {}

pub fn get_host_and_file(remote_file_url: &str) -> Result<(String, String), Box<dyn Error>> {
    let parts: Vec<&str> = remote_file_url.split('|').collect();
    
    if parts.len() != 2 {
        return Err(Box::new(InvalidRemoteFileUrl {
            message: format!("unknown remoteFileUrl: {}", remote_file_url),
        }));
    }
    
    Ok((parts[0].to_string(), parts[1].to_string()))
}