use std::sync::Arc;
use std::thread;

use anyhow::{anyhow, Result};
use crossbeam_channel::bounded;

use venus_worker::{
    infra::objstore::filestore::FileStore,
    logging::info,
    sealing::{config, resource, store::StoreManager},
};

pub fn start_deamon(cfg_path: String) -> Result<()> {
    let cfg = config::Config::load(&cfg_path)?;
    info!("config loaded\n {:?}", cfg);

    let remote_store = cfg
        .remote
        .path
        .ok_or(anyhow!("remote path is required for mock"))?;
    let remote = Arc::new(FileStore::open(remote_store)?);

    let limit = resource::Pool::new(cfg.limit.iter());

    let store_mgr = StoreManager::load(&cfg.store, &cfg.sealing)?;

    // let (done_tx, done_rx) = bounded(0);

    let (mgr_stop_tx, mgr_stop_rx) = bounded::<()>(0);
    // thread::spawn(move || {
    //     info!("store mgr start");
    //     store_mgr.start_sealing(done_rx, Arc::new(mock_client), remote, Arc::new(limit));
    //     drop(mgr_stop_tx);
    // });

    // drop(done_tx);

    unimplemented!();
}
