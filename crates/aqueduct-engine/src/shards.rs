//! Many partitions in one process (ADR 0057): one [`Engine`] per partition, each owning the keys whose
//! home's slot the slot table gives it (ADR 0052), and calls routed by key. Messages between partitions
//! (children with chosen keys, messages to objects) are handed from one engine to another. With one
//! partition, it is the plain engine, in its data directory as before.
//!
//! The partitions share one step queue and one activation queue (if the builder sets them, with
//! [`Builder::shared_step_queue`]), so that workers poll one queue for every partition.

use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use aqueduct_core::{Payload, RunState};

use crate::embedded::{
    Builder, Engine, EngineError, EngineStats, Escalation, RaftInbox, RemoteRoute, RunResult,
    RunSummary,
};
use crate::placement::{SlotTable, home};
use crate::{Remote, StartOptions};

/// One engine per partition.
pub struct Shards {
    table: SlotTable,
    engines: Vec<Engine>,
}

impl std::fmt::Debug for Shards {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shards")
            .field("partitions", &self.engines.len())
            .finish()
    }
}

impl Shards {
    /// One partition: the engine as it is.
    pub fn single(engine: Engine) -> Shards {
        Shards {
            table: SlotTable::even(1),
            engines: vec![engine],
        }
    }

    /// Opens `partitions` partitions, each in `dir/partitions/<index>` with the builder `build` gives
    /// for that directory and index; one partition is opened in `dir` itself. Messages between
    /// partitions are handed over in this process.
    pub fn open(
        dir: &Path,
        partitions: u32,
        build: impl FnMut(PathBuf, u32) -> Builder,
    ) -> Result<Shards, EngineError> {
        Shards::open_routed(dir, partitions, build, |_| None)
    }

    /// [`Shards::open`], with a say in where messages for another partition go: `route(partition)`
    /// returns a route for it (for example, to the node that leads it), or `None` to hand them to this
    /// process's engine for it.
    pub fn open_routed(
        dir: &Path,
        partitions: u32,
        mut build: impl FnMut(PathBuf, u32) -> Builder,
        route: impl Fn(u32) -> Option<RemoteRoute>,
    ) -> Result<Shards, EngineError> {
        if partitions <= 1 {
            return Ok(Shards::single(build(dir.to_path_buf(), 0).open()?));
        }
        let table = SlotTable::even(partitions);
        let inboxes: Arc<RwLock<Vec<RaftInbox>>> = Arc::default();
        let routes: Vec<Option<RemoteRoute>> = (0..partitions).map(route).collect();
        let deliver: RemoteRoute = {
            let table = table.clone();
            let inboxes = Arc::clone(&inboxes);
            Arc::new(move |to: String, message: Remote| {
                let partition = table.partition_of(&to);
                if let Some(route) = &routes[partition as usize] {
                    route(to, message);
                } else if let Some(inbox) = inboxes
                    .read()
                    .ok()
                    .and_then(|inboxes| inboxes.get(partition as usize).cloned())
                {
                    inbox.deliver_remote(message);
                }
            })
        };
        let mut engines = Vec::new();
        for partition in 0..partitions {
            let path = dir.join("partitions").join(partition.to_string());
            let engine = build(path, partition)
                .partition_of(table.placement(partition), Arc::clone(&deliver))
                .open()?;
            if let Ok(mut inboxes) = inboxes.write() {
                inboxes.push(engine.raft_inbox());
            }
            engines.push(engine);
        }
        Ok(Shards { table, engines })
    }

    /// The slot table.
    pub fn table(&self) -> &SlotTable {
        &self.table
    }

    /// The engines, by partition.
    pub fn engines(&self) -> &[Engine] {
        &self.engines
    }

    /// The engine of the partition a key, run id, awakeable or escalation id lives on.
    pub fn engine_for(&self, key: &str) -> &Engine {
        let partition = self.table.partition_of(home(key)) as usize;
        &self.engines[partition.min(self.engines.len() - 1)]
    }

    fn all<T>(
        &self,
        each: impl Fn(&Engine) -> Result<Vec<T>, EngineError>,
    ) -> Result<Vec<T>, EngineError> {
        let mut out = Vec::new();
        for engine in &self.engines {
            out.extend(each(engine)?);
        }
        Ok(out)
    }

    /// See [`Engine::start_with`].
    pub fn start_with(
        &self,
        key: &str,
        workflow: &str,
        input: Payload,
        options: impl Into<StartOptions>,
    ) -> Result<(), EngineError> {
        self.engine_for(key)
            .start_with(key, workflow, input, options)
    }

    /// See [`Engine::start`].
    pub fn start(&self, key: &str, workflow: &str, input: Payload) -> Result<(), EngineError> {
        self.engine_for(key).start(key, workflow, input)
    }

    /// See [`Engine::wait`].
    pub fn wait(&self, key: &str, timeout: Duration) -> Result<RunResult, EngineError> {
        self.engine_for(key).wait(key, timeout)
    }

    /// See [`Engine::signal`].
    pub fn signal(
        &self,
        key: &str,
        name: &str,
        id: &str,
        payload: Payload,
    ) -> Result<(), EngineError> {
        self.engine_for(key).signal(key, name, id, payload)
    }

    /// See [`Engine::update`].
    pub fn update(
        &self,
        key: &str,
        name: &str,
        id: &str,
        payload: Payload,
        timeout: Duration,
    ) -> Result<Payload, EngineError> {
        self.engine_for(key).update(key, name, id, payload, timeout)
    }

    /// See [`Engine::query`].
    pub fn query(&self, key: &str, name: &str) -> Result<Payload, EngineError> {
        self.engine_for(key).query(key, name)
    }

    /// See [`Engine::describe`].
    pub fn describe(&self, key: &str) -> Result<(String, RunState), EngineError> {
        self.engine_for(key).describe(key)
    }

    /// Every partition's runs.
    pub fn runs(&self) -> Result<Vec<RunSummary>, EngineError> {
        self.all(Engine::runs)
    }

    /// See [`Engine::call_object`].
    pub fn call_object(
        &self,
        object: &str,
        key: &str,
        handler: &str,
        id: &str,
        args: &Payload,
        timeout: Duration,
    ) -> Result<Payload, EngineError> {
        self.engine_for(&crate::objects::object_run_key(object, key))
            .call_object(object, key, handler, id, args, timeout)
    }

    /// See [`Engine::object_state`].
    pub fn object_state(&self, object: &str, key: &str) -> Result<Payload, EngineError> {
        self.engine_for(&crate::objects::object_run_key(object, key))
            .object_state(object, key)
    }

    /// See [`Engine::cancel`].
    pub fn cancel(&self, key: &str, reason: &str) -> Result<(), EngineError> {
        self.engine_for(key).cancel(key, reason)
    }

    /// Every partition's open escalations.
    pub fn escalations(&self) -> Result<Vec<Escalation>, EngineError> {
        self.all(Engine::escalations)
    }

    /// See [`Engine::resolve_escalation`].
    pub fn resolve_escalation(&self, id: &str, outcome: Payload) -> Result<(), EngineError> {
        self.engine_for(id).resolve_escalation(id, outcome)
    }

    /// See [`Engine::complete_awakeable`].
    pub fn complete_awakeable(&self, id: &str, payload: Payload) -> Result<(), EngineError> {
        self.engine_for(id).complete_awakeable(id, payload)
    }

    /// See [`Engine::set_priority`].
    pub fn set_priority(&self, key: &str, priority: u32) -> Result<(), EngineError> {
        self.engine_for(key).set_priority(key, priority)
    }

    /// See [`Engine::history`].
    pub fn history(&self, run: &str) -> Result<Option<Vec<aqueduct_core::Entry>>, EngineError> {
        self.engine_for(run).history(run)
    }

    /// See [`Engine::step_result`].
    pub fn step_result(
        &self,
        run: &str,
        cmd: aqueduct_core::CommandIndex,
        attempt: crate::embedded::Attempt,
    ) -> Result<(), EngineError> {
        self.engine_for(run).step_result(run, cmd, attempt)
    }

    /// See [`Engine::decision`].
    pub fn decision(
        &self,
        run: &str,
        at: usize,
        commands: Vec<aqueduct_core::Command>,
    ) -> Result<(), EngineError> {
        self.engine_for(run).decision(run, at, commands)
    }

    /// The queue of remote steps, shared by every partition.
    pub fn step_queue(&self) -> Option<&Arc<crate::queue::StepQueue>> {
        self.engines[0].step_queue()
    }

    /// The queue of activations, shared by every partition.
    pub fn activation_queue(&self) -> Option<&Arc<crate::activations::ActivationQueue>> {
        self.engines[0].activation_queue()
    }

    /// Every build of every remote workflow, as the first partition has them (all have the same).
    pub fn deployments(&self) -> Result<Vec<crate::deployments::DeploymentInfo>, EngineError> {
        self.engines[0].deployments()
    }

    /// See [`Engine::activate_deployment`]: on every partition.
    pub fn activate_deployment(
        &self,
        workflow: &str,
        build: &str,
        force: bool,
    ) -> Result<(), EngineError> {
        for engine in &self.engines {
            engine.activate_deployment(workflow, build, force)?;
        }
        Ok(())
    }

    /// See [`Engine::retire_deployment`]: on every partition.
    pub fn retire_deployment(&self, workflow: &str, build: &str) -> Result<(), EngineError> {
        for engine in &self.engines {
            engine.retire_deployment(workflow, build)?;
        }
        Ok(())
    }

    /// Every partition's statistics, added up.
    pub fn stats(&self) -> Result<EngineStats, EngineError> {
        let mut total = EngineStats {
            runs_in_memory: 0,
            pending_timers: 0,
            log_lsn: 0,
        };
        for engine in &self.engines {
            let stats = engine.stats()?;
            total.runs_in_memory += stats.runs_in_memory;
            total.pending_timers += stats.pending_timers;
            total.log_lsn += stats.log_lsn;
        }
        Ok(total)
    }
}
