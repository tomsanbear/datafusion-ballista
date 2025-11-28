// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#![doc = include_str ! ("../README.md")]
#[cfg(feature = "rest-api")]
pub mod api;
pub mod cluster;
pub mod config;
pub mod display;
pub mod metrics;
pub mod physical_optimizer;
pub mod planner;
pub mod scheduler_process;
pub mod scheduler_server;
pub mod standalone;
pub mod state;

#[cfg(test)]
pub mod test_utils;

pub use scheduler_server::SessionBuilder;

use std::sync::Arc;

/// Context for a single task's extension data, passed to the reducer.
///
/// Contains metadata about which task produced the extension data,
/// allowing the reducer to make aggregation decisions based on
/// stage, partition, or executor information.
#[derive(Debug, Clone)]
pub struct TaskExtensionEntry {
    /// The job ID this task belongs to
    pub job_id: String,
    /// The stage ID within the job
    pub stage_id: usize,
    /// The partition ID within the stage
    pub partition_id: usize,
    /// The executor ID that ran this task
    pub executor_id: String,
    /// The serialized extension data from the task
    pub data: Vec<u8>,
}

/// Hook called when a job completes successfully to reduce all task
/// extension data into a single job-level extension.
///
/// This hook receives all [`TaskExtensionEntry`] instances collected during
/// job execution and returns aggregated bytes for `SuccessfulJob.extension`.
///
/// # Example
///
/// ```ignore
/// let reducer: JobExtensionReducer = Arc::new(|entries: Vec<TaskExtensionEntry>| {
///     // Sum up metrics from all tasks
///     let mut total = MyMetrics::default();
///     for entry in entries {
///         if let Ok(task_metrics) = serde_json::from_slice::<MyMetrics>(&entry.data) {
///             total.bytes_read += task_metrics.bytes_read;
///             total.rows_processed += task_metrics.rows_processed;
///         }
///     }
///     serde_json::to_vec(&total).ok()
/// });
/// ```
pub type JobExtensionReducer =
    Arc<dyn Fn(Vec<TaskExtensionEntry>) -> Option<Vec<u8>> + Send + Sync>;
