use serde::{Deserialize, Serialize};
use std::time::Duration;
use vc_processors::fil_proofs::{
    to_prover_id, SealCommitPhase1Output, SealCommitPhase2Output, SealPreCommitPhase1Output, SealPreCommitPhase2Output, SectorId,
};

use anyhow::{anyhow, Context, Result};

use crate::{
    metadb::{rocks::RocksMeta, MaybeDirty, MetaDocumentDB, PrefixedMetaDB, Saved},
    rpc::sealer::{
        AcquireDealsSpec, AllocateSectorSpec, AllocatedSector, Deals, OnChainState, PieceInfo, PreCommitOnChainInfo, ProofOnChainInfo,
        SealerClient, Seed, SubmitResult, Ticket, WorkerIdentifier,
    },
    sealing::{
        failure::{Failure, IntoFailure, MapErrToFailure, TaskAborted},
        sealing_thread::{planner::batch::sectors::Base, util::call_rpc, SealingCtrl},
    },
    store::Store,
};

use self::sectors::{Sector, Sectors};

use super::{common::sector::Trace, JobTrait, PlannerTrait};

mod sectors;

pub(crate) struct Job {
    pub sectors: Saved<Sectors, &'static str, PrefixedMetaDB<&'static RocksMeta>>,
    _trace: Vec<Trace>,

    pub sealing_ctrl: SealingCtrl<'static>,
    store: &'static Store,
    ident: WorkerIdentifier,

    _trace_meta: MetaDocumentDB<PrefixedMetaDB<&'static RocksMeta>>,
}

impl Job {
    pub fn rpc(&self) -> &SealerClient {
        self.sealing_ctrl.ctx.global.rpc.as_ref()
    }

    pub fn sector(&self, index: usize) -> Result<&Sector> {
        self.sectors
            .sectors
            .get(index)
            .with_context(|| format!("sector index out of bounds: {}", index))
    }
}

impl JobTrait for Job {
    fn planner(&self) -> &str {
        // Batch planner does not support switching palnner
        "batch"
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub enum State {
    Empty,
    Allocated,
    DealsAcquired { index: usize },
    PieceAdded { index: usize },
    TreeDBuilt { index: usize },
    TicketAssigned,
    PC1Done,
    PC2Done,
    PCSubmitted { index: usize },
    PCLanded { index: usize },
    Persisted { index: usize },
    PersistanceSubmitted { index: usize },
    SeedAssigned { index: usize },
    C1Done,
    C2Done { index: usize },
    ProofSubmitted { index: usize },
    Finished { index: usize },
    Aborted,
}

#[derive(Debug)]
pub enum Event {
    SetState(State),
    // No specified tasks available from sector_manager.
    Idle,
    Allocate(Vec<AllocatedSector>),
    AcquireDeals { index: usize, deals: Option<Deals> },
    AddPiece { index: usize, pieces: Vec<PieceInfo> },
    BuildTreeD { index: usize },
    AssignTicket(Ticket),
    PC1(Ticket, Vec<SealPreCommitPhase1Output>),
    PC2(Vec<SealPreCommitPhase2Output>),
    SubmitPC { index: usize },
    ReSubmitPC { index: usize },
    CheckPC { index: usize },
    Persist { instance: String, index: usize },
    SubmitPersistance { index: usize },
    AssignSeed { index: usize, seed: Seed },
    C1(Vec<SealCommitPhase1Output>),
    C2 { index: usize, out: SealCommitPhase2Output },
    SubmitProof { index: usize },
    ReSubmitProof { index: usize },
    Finish { index: usize },
}

impl Event {
    fn apply(self, state: State, job: &mut Job) -> Result<()> {
        let next = if let Event::SetState(s) = &self { s.clone() } else { state };

        if next == job.sectors.state {
            return Err(anyhow!("state unchanged, may enter an infinite loop"));
        }

        self.apply_changes(job.sectors.inner_mut());
        // task.sector.update_state(next);

        Ok(())
    }

    fn apply_changes(self, s: &mut MaybeDirty<Sectors>) {
        match self {
            Self::Allocate(sectors) => {
                for (allocated, sector) in sectors.into_iter().zip(s.sectors.iter_mut()) {
                    let prover_id = to_prover_id(allocated.id.miner);
                    let sector_id = SectorId::from(allocated.id.number);

                    let base = Base {
                        allocated,
                        prove_input: (prover_id, sector_id),
                    };
                    sector.base.replace(base);
                }
            }

            Self::AcquireDeals { index, deals } => {
                if let Some(sector) = s.sectors.get_mut(index) {
                    sector.deals = deals;
                }
            }

            // Self::AddPiece(pieces) => {
            //     replace!(s.phases.pieces, pieces);
            // }
            Self::BuildTreeD { .. } => {}
            Self::AssignTicket(ticket) => {
                for sector in &mut s.sectors {
                    sector.phases.ticket.replace(ticket.clone());
                }
            }

            // Self::PC1(ticket, out) => {
            //     if s.phases.ticket.as_ref() != Some(&ticket) {
            //         replace!(s.phases.ticket, ticket);
            //     }
            //     replace!(s.phases.pc1out, out);
            // }

            // Self::PC2(out) => {
            //     replace!(s.phases.pc2out, out);
            // }
            Self::Persist { instance, index } => {
                if let Some(sector) = s.sectors.get_mut(index) {
                    sector.phases.persist_instance.replace(instance);
                }
            }

            Self::AssignSeed { index, seed } => {
                if let Some(sector) = s.sectors.get_mut(index) {
                    sector.phases.seed.replace(seed);
                }
            }

            Self::C1(out) => {
                for (sector, c1out) in s.sectors.iter_mut().zip(out) {
                    sector.phases.c1out.replace(c1out);
                }
            }

            Self::C2 { index, out } => {
                if let Some(sector) = s.sectors.get_mut(index) {
                    sector.phases.c2out.replace(out);
                }
            }

            Self::SubmitPC { index } => {
                if let Some(sector) = s.sectors.get_mut(index) {
                    sector.phases.pc2_re_submit = false
                }
            }

            Self::ReSubmitPC { index } => {
                if let Some(sector) = s.sectors.get_mut(index) {
                    sector.phases.pc2_re_submit = true
                }
            }

            Self::SubmitProof { index } => {
                if let Some(sector) = s.sectors.get_mut(index) {
                    sector.phases.c2_re_submit = false
                }
            }

            Self::ReSubmitProof { index } => {
                if let Some(sector) = s.sectors.get_mut(index) {
                    sector.phases.c2_re_submit = true
                }
            }

            _ => {}
        };
    }
}

#[derive(Default)]
pub(crate) struct BatchPlanner {
    batch_size: usize,
}

impl PlannerTrait for BatchPlanner {
    type Job = Job;
    type State = State;
    type Event = Event;

    fn name(&self) -> &str {
        "batch"
    }

    fn plan(&self, evt: &Self::Event, st: &Self::State) -> Result<Self::State> {
        Ok(match (st, evt) {
            (State::Empty, Event::Allocate { .. }) => State::Allocated,
            (State::Allocated, Event::AcquireDeals { index, .. }) | (State::DealsAcquired { .. }, Event::AcquireDeals { index, .. }) => {
                State::DealsAcquired { index: *index }
            }
            (State::DealsAcquired { .. }, Event::AddPiece { index, .. }) | (State::PieceAdded { .. }, Event::AddPiece { index, .. }) => {
                State::PieceAdded { index: *index }
            }
            (State::PieceAdded { .. }, Event::BuildTreeD { index }) | (State::TreeDBuilt { .. }, Event::BuildTreeD { index }) => {
                State::TreeDBuilt { index: *index }
            }
            (State::TreeDBuilt { .. }, Event::AssignTicket { .. }) => State::TicketAssigned,
            (State::TicketAssigned { .. }, Event::PC1(..)) => State::PC1Done,
            (State::PC1Done, Event::PC2(..)) => State::PC2Done,
            (State::PC2Done, Event::SubmitPC { index }) | (State::PCSubmitted { .. }, Event::SubmitPC { index }) => {
                State::PCSubmitted { index: *index }
            }
            (State::PCSubmitted { .. }, Event::CheckPC { index }) | (State::PCLanded { .. }, Event::CheckPC { index }) => {
                State::PCLanded { index: *index }
            }
            (State::PCSubmitted { .. }, Event::ReSubmitPC { index }) => {
                if *index > 0 {
                    State::PCSubmitted { index: index - 1 }
                } else {
                    State::PC2Done
                }
            }
            (State::PCLanded { .. }, Event::Persist { index, .. }) | (State::Persisted { .. }, Event::SubmitPersistance { index, .. }) => {
                State::Persisted { index: *index }
            }
            (State::PersistanceSubmitted { .. }, Event::AssignSeed { index, .. })
            | (State::SeedAssigned { .. }, Event::AssignSeed { index, .. }) => State::SeedAssigned { index: *index },

            (State::SeedAssigned { .. }, Event::C1(_)) => State::C1Done,
            (State::C1Done { .. }, Event::C2 { index, .. }) | (State::C2Done { .. }, Event::C2 { index, .. }) => {
                State::C2Done { index: *index }
            }
            (State::C2Done { .. }, Event::SubmitProof { index }) | (State::ProofSubmitted { .. }, Event::SubmitProof { index }) => {
                State::ProofSubmitted { index: *index }
            }
            (State::ProofSubmitted { .. }, Event::ReSubmitProof { index }) => {
                if *index > 0 {
                    State::ProofSubmitted { index: index - 1 }
                } else {
                    State::C2Done {
                        index: self.batch_size - 1,
                    }
                }
            }
            (State::ProofSubmitted { .. }, Event::Finish { index }) | (State::Finished { .. }, Event::Finish { index }) => {
                State::Finished { index: *index }
            }
            _ => {
                return Err(anyhow::anyhow!("unexpected state and event {:?} {:?}", st, evt));
            }
        })
    }

    fn exec(&self, job: &mut Self::Job) -> Result<Option<Self::Event>, Failure> {
        let state = job.sectors.state.clone();
        let batch_size = job.sectors.batch_size;

        let inner = BatchSealer { job };

        match state {
            State::Empty => inner.allocate(),
            State::Allocated => inner.acquire_deals(0),
            State::DealsAcquired { index } if index < batch_size - 1 => inner.acquire_deals(index + 1),
            State::DealsAcquired { .. } => inner.add_pieces(0),
            State::PieceAdded { index } if index < batch_size - 1 => inner.build_tree_d(),
            State::PieceAdded { .. } => inner.build_tree_d(),
            State::TreeDBuilt { .. } => inner.assign_ticket(),
            State::TicketAssigned => inner.pc1(),
            State::PC1Done => inner.pc2(),
            State::PC2Done => inner.submit_pre_commit(0),
            State::PCSubmitted { index } if index < batch_size - 1 => inner.submit_pre_commit(index + 1),
            State::PCSubmitted { .. } => inner.check_pre_commit_state(0),
            State::PCLanded { index } if index < batch_size - 1 => inner.check_pre_commit_state(index + 1),
            State::PCLanded { .. } => inner.persist_sector_files(0),
            State::Persisted { index } if index < batch_size - 1 => inner.persist_sector_files(index),
            State::Persisted { .. } => inner.submit_persisted(0),
            State::PersistanceSubmitted { index } if index < batch_size - 1 => inner.submit_persisted(index + 1),
            State::PersistanceSubmitted { .. } => inner.wait_seed(0),
            State::SeedAssigned { index } if index < batch_size - 1 => inner.wait_seed(index + 1),
            State::SeedAssigned { .. } => inner.commit1(),
            State::C1Done => inner.commit2(0),
            State::C2Done { index } if index < batch_size - 1 => inner.commit2(index),
            State::C2Done { .. } => inner.submit_proof(0),
            State::ProofSubmitted { index } if index < batch_size - 1 => inner.submit_proof(index + 1),
            State::ProofSubmitted { .. } => inner.check_proof_state(0),
            State::Finished { index } if index < batch_size - 1 => inner.check_proof_state(index + 1),
            State::Finished { .. } => return Ok(None),
            State::Aborted => return Err(TaskAborted.into()),
        }
        .map(Some)
    }

    fn apply(&self, event: Self::Event, state: Self::State, job: &mut Self::Job) -> Result<()> {
        todo!()
    }
}

struct BatchSealer<'a> {
    job: &'a mut Job,
}

impl BatchSealer<'_> {
    pub fn allocate(&self) -> Result<Event, Failure> {
        let maybe_allocated_res = call_rpc! {
            self.job.rpc()=>allocate_sectors_batch(AllocateSectorSpec {
                allowed_miners: Some(self.job.sealing_ctrl.config().allowed_miners.clone()),
                allowed_proof_types: Some(self.job.sealing_ctrl.config().allowed_proof_types.clone()),
                },
                self.job.sectors.batch_size as u32,
            )
        };

        let allocated = match maybe_allocated_res {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!("sectors are not allocated yet, so we can retry even though we got the err {:?}", e);
                return Ok(Event::Idle);
            }
        };

        if allocated.is_empty() {
            return Ok(Event::Idle);
        }

        Ok(Event::Allocate(allocated))
    }

    pub fn acquire_deals(&self, index: usize) -> Result<Event, Failure> {
        let disable_cc = self.job.sealing_ctrl.config().disable_cc;

        if !self.job.sealing_ctrl.config().enable_deals {
            return Ok(if disable_cc {
                Event::Idle
            } else {
                Event::AcquireDeals {
                    index: self.job.sectors.sectors.len(),
                    deals: None,
                }
            });
        }
        let spec = AcquireDealsSpec {
            max_deals: self.job.sealing_ctrl.config().max_deals,
            min_used_space: self.job.sealing_ctrl.config().min_deal_space.map(|b| b.get_bytes() as usize),
        };

        let sector = self.job.sector(index).crit()?;
        let sector_id = sector.base.as_ref().context("sector base required").crit()?.allocated.id.clone();

        let deals = call_rpc! {
            self.job.rpc()=>acquire_deals(
                sector_id,
                spec,
            )
        }?;

        let deals_count = deals.as_ref().map(|d| d.len()).unwrap_or(0);

        tracing::debug!(count = deals_count, "pieces acquired");
        Ok(if disable_cc || deals_count > 0 {
            Event::AcquireDeals { index, deals }
        } else {
            Event::Idle
        })
    }

    fn add_pieces(&self, index: usize) -> Result<Event, Failure> {
        todo!()
    }

    fn build_tree_d(&self) -> Result<Event, Failure> {
        todo!()
    }

    fn assign_ticket(&self) -> Result<Event, Failure> {
        let sector = self.job.sector(0).crit()?;
        let sector_id = sector.base.as_ref().context("sector base required").crit()?.allocated.id.clone();

        let ticket = match &sector.phases.ticket {
            // Use the existing ticket when rebuilding sectors
            Some(ticket) => ticket.clone(),
            None => {
                let ticket = call_rpc! {
                    self.job.rpc() => assign_ticket(sector_id,)
                }?;
                tracing::debug!(ticket = ?ticket.ticket.0, epoch = ticket.epoch, "ticket assigned from sector-manager");
                ticket
            }
        };

        Ok(Event::AssignTicket(ticket))
    }

    fn pc1(&self) -> Result<Event, Failure> {
        todo!()
    }

    fn pc2(&self) -> Result<Event, Failure> {
        todo!()
    }

    fn submit_pre_commit(&self, index: usize) -> Result<Event, Failure> {
        let sector = self.job.sector(index).crit()?;

        let (sector_id, comm_r, comm_d, ticket) =
            if let (Some(base), Some(pc2out), Some(ticket)) = (&sector.base, &sector.phases.pc2out, sector.phases.ticket.clone()) {
                (base.allocated.clone(), pc2out.comm_r, pc2out.comm_d, ticket)
            } else {
                return Err(anyhow!("PC2 not completed").crit());
            };

        let deals = sector.deals.as_ref().map(|x| x.iter().map(|x| x.id).collect()).unwrap_or_default();

        let pinfo = PreCommitOnChainInfo {
            comm_r,
            comm_d,
            ticket,
            deals,
        };

        let res = call_rpc! {
            self.job.rpc() => submit_pre_commit(sector_id, pinfo, sector.phases.pc2_re_submit,)
        }?;

        // TODO: handle submit reset correctly
        match res.res {
            SubmitResult::Accepted | SubmitResult::DuplicateSubmit => Ok(Event::SubmitPC { index }),

            SubmitResult::MismatchedSubmission => Err(anyhow!("{:?}: {:?}", res.res, res.desc).perm()),

            SubmitResult::Rejected => Err(anyhow!("{:?}: {:?}", res.res, res.desc).abort()),

            SubmitResult::FilesMissed => Err(anyhow!("FilesMissed should not happen for pc2 submission: {:?}", res.desc).perm()),
        }
    }

    fn check_pre_commit_state(&self, index: usize) -> Result<Event, Failure> {
        let sector = self.job.sector(index).crit()?;
        let sector_id = sector.base.as_ref().map(|b| &b.allocated.id).context("context").crit()?;

        loop {
            let state = call_rpc! {
                self.job.rpc()=>poll_pre_commit_state(sector_id.clone(), )
            }?;

            match state.state {
                OnChainState::Landed => break,
                OnChainState::NotFound => return Err(anyhow!("pre commit on-chain info not found").perm()),

                OnChainState::Failed => {
                    tracing::warn!("pre commit on-chain info failed: {:?}", state.desc);
                    // TODO: make it configurable
                    self.job.sealing_ctrl.wait_or_interrupted(Duration::from_secs(30))?;
                    return Ok(Event::ReSubmitPC { index });
                }

                OnChainState::PermFailed => return Err(anyhow!("pre commit on-chain info permanent failed: {:?}", state.desc).perm()),

                OnChainState::ShouldAbort => return Err(anyhow!("pre commit info will not get on-chain: {:?}", state.desc).abort()),

                OnChainState::Pending | OnChainState::Packed => {}
            }

            tracing::debug!(
                state = ?state.state,
                interval = ?self.job.sealing_ctrl.config().rpc_polling_interval,
                "waiting for next round of polling pre commit state",
            );

            self.job
                .sealing_ctrl
                .wait_or_interrupted(self.job.sealing_ctrl.config().rpc_polling_interval)?;
        }

        tracing::debug!(index = index, "pre commit landed");

        Ok(Event::CheckPC { index })
    }

    fn persist_sector_files(&self, index: usize) -> Result<Event, Failure> {
        let sector_id = self.job.sector_id()?;

        field_required! {
            instance,
            task.sector.phases.persist_instance.as_ref().cloned()
        }

        let checked = call_rpc! {
            self.job.rpc() => submit_persisted_ex(sector_id.clone(), instance, is_upgrade,)
        }?;

        if checked {
            Ok(())
        } else {
            Err(anyhow!("sector files are persisted but unavailable for sealer")).perm()
        }
    }

    fn submit_persisted(&self, index: usize) -> Result<Event, Failure> {
        let sector = self.job.sector(index).crit()?;

        let sector_id = sector.base.as_ref().context("sector base required").crit()?.allocated.id.clone();
        let persist_instance = sector
            .phases
            .persist_instance
            .clone()
            .context("sector persist instance required")
            .crit()?;

        let checked = call_rpc! {
            self.job.rpc() => submit_persisted_ex(sector_id.clone(), persist_instance, false,)
        }?;

        if checked {
            Ok(Event::SubmitPersistance { index })
        } else {
            Err(anyhow!("sector files are persisted but unavailable for sealer")).perm()
        }
    }

    fn wait_seed(&self, index: usize) -> Result<Event, Failure> {
        let sector = self.job.sector(index).crit()?;
        let sector_id = sector.base.as_ref().context("sector base required").crit()?.allocated.id.clone();

        let seed = loop {
            let wait = call_rpc! {
                self.job.rpc()=>wait_seed(sector_id.clone(), )
            }?;

            if let Some(seed) = wait.seed {
                break seed;
            };

            if !wait.should_wait || wait.delay == 0 {
                return Err(anyhow!("invalid empty wait_seed response").temp());
            }

            let delay = Duration::from_secs(wait.delay);

            tracing::debug!(?delay, "waiting for next round of polling seed");

            self.job.sealing_ctrl.wait_or_interrupted(delay)?;
        };

        Ok(Event::AssignSeed { index, seed })
    }

    fn commit1(&self) -> Result<Event, Failure> {
        // cloned_required! {
        //     seed,
        //     self.task.sector.phases.seed
        // }

        // common::commit1_with_seed(self.task, seed).map(Event::C1)
        todo!()
    }

    fn commit2(&self, index: usize) -> Result<Event, Failure> {
        todo!()
    }

    fn submit_proof(&self, index: usize) -> Result<Event, Failure> {
        let sector = self.job.sector(index).crit()?;
        let sector_id = sector.base.as_ref().context("sector base required").crit()?.allocated.id.clone();

        let proof = sector.phases.c2out.clone().context("c2out required").crit()?;

        let info = ProofOnChainInfo { proof: proof.proof.into() };

        let res = call_rpc! {
            self.job.rpc()=>submit_proof(sector_id, info, sector.phases.c2_re_submit,)
        }?;

        // TODO: submit reset correctly
        match res.res {
            SubmitResult::Accepted | SubmitResult::DuplicateSubmit => Ok(Event::SubmitProof { index }),

            SubmitResult::MismatchedSubmission => Err(anyhow!("{:?}: {:?}", res.res, res.desc).perm()),

            SubmitResult::Rejected => Err(anyhow!("{:?}: {:?}", res.res, res.desc).abort()),

            SubmitResult::FilesMissed => Err(anyhow!("FilesMissed is not handled currently: {:?}", res.desc).perm()),
        }
    }

    fn check_proof_state(&self, index: usize) -> Result<Event, Failure> {
        let sector = self.job.sector(index).crit()?;
        let sector_id = sector.base.as_ref().context("sector base required").crit()?.allocated.id.clone();

        if !self.job.sealing_ctrl.config().ignore_proof_check {
            loop {
                let state = call_rpc! {
                    self.job.rpc() => poll_proof_state(sector_id.clone(),)
                }?;

                match state.state {
                    OnChainState::Landed => break,
                    OnChainState::NotFound => return Err(anyhow!("proof on-chain info not found").perm()),

                    OnChainState::Failed => {
                        tracing::warn!("proof on-chain info failed: {:?}", state.desc);
                        // TODO: make it configurable
                        self.job.sealing_ctrl.wait_or_interrupted(Duration::from_secs(30))?;
                        return Ok(Event::ReSubmitProof { index });
                    }

                    OnChainState::PermFailed => return Err(anyhow!("proof on-chain info permanent failed: {:?}", state.desc).perm()),

                    OnChainState::ShouldAbort => return Err(anyhow!("sector will not get on-chain: {:?}", state.desc).abort()),

                    OnChainState::Pending | OnChainState::Packed => {}
                }

                tracing::debug!(
                    state = ?state.state,
                    interval = ?self.job.sealing_ctrl.config().rpc_polling_interval,
                    "waiting for next round of polling proof state",
                );

                self.job
                    .sealing_ctrl
                    .wait_or_interrupted(self.job.sealing_ctrl.config().rpc_polling_interval)?;
            }
        }

        // let cache_dir = self.job.cache_dir(sector_id);
        // let sector_size = allocated.proof_type.sector_size();

        // we should be careful here, use failure as temporary
        // clear_cache(sector_size, cache_dir.as_ref()).temp()?;
        // debug!(
        //     dir = ?&cache_dir,
        //     "clean up unnecessary cached files"
        // );

        Ok(Event::Finish { index })
    }
}
