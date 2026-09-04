module HCurl.Agent (
    Agent,
    AgentClosed (..),
    ManagedMetrics (..),
    ManagedPolicy (..),
    MetricsHook,
    agentWorkerCount,
    closeAgent,
    defaultManagedPolicy,
    registerManagedMetrics,
    sampleManagedMetrics,
    spawnAgent,
    spawnManagedAgent,
    spawnThreadedAgent,
    withAgent,
    withManagedAgent,
    withThreadedAgent,
)
where

import HCurl.Internal.Agent
