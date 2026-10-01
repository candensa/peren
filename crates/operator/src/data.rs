use crate::{
    cli,
    run::{CliError, ProcessEnvironment},
};

pub(super) fn d1(command: cli::D1) -> Result<(), CliError> {
    match command {
        cli::D1::PruneHistory {
            database,
            retention_days: _,
            dry_run,
        } => {
            let config = peren_config::FleetConfig::from_path(database.config)?.validate()?;
            let report = peren_node::prune_d1(
                &config,
                &ProcessEnvironment,
                &peren_node::D1Prune {
                    service: database.service,
                    binding: database.binding,
                    dry: dry_run,
                },
            )?;
            if report.dry {
                println!(
                    "would prune {} histor{}",
                    report.pruned,
                    plural(report.pruned)
                );
            } else {
                println!("pruned {} histor{}", report.pruned, plural(report.pruned));
            }
        }
        cli::D1::Migrate { database, dir } => {
            let config = peren_config::FleetConfig::from_path(database.config)?.validate()?;
            let report = peren_node::migrate_d1(
                &config,
                &ProcessEnvironment,
                &peren_node::D1Migration {
                    service: database.service,
                    binding: database.binding,
                    dir,
                },
            )?;
            println!(
                "applied {} migration{}",
                report.files.len(),
                suffix(report.files.len())
            );
        }
        cli::D1::Query { database, sql } => {
            let config = peren_config::FleetConfig::from_path(database.config)?.validate()?;
            let output = peren_node::query_d1(
                &config,
                &ProcessEnvironment,
                &peren_node::D1Query {
                    service: database.service,
                    binding: database.binding,
                    sql,
                },
            )?;
            match output {
                peren_node::D1QueryOutput::Rows(rows) => {
                    for row in rows {
                        println!("{row}");
                    }
                }
                peren_node::D1QueryOutput::Metadata(metadata) => println!("{metadata}"),
            }
        }
        cli::D1::Restore(_) => {
            return Err(CliError::Unsupported(
                "d1 restore requires D1 time-travel snapshots and bookmarks, which are not available in the current native storage model",
            ));
        }
    }
    Ok(())
}

pub(super) async fn queue(command: cli::Queue) -> Result<(), CliError> {
    match command {
        cli::Queue::Depth { config, queue } => {
            let config = peren_config::FleetConfig::from_path(config)?.validate()?;
            let report =
                peren_node::queue_depth(&config, &peren_node::QueueDepth { queue }).await?;
            println!(
                "queue {} ready={} delayed={} leased={} paused={}",
                report.queue, report.ready, report.delayed, report.leased, report.paused
            );
        }
        cli::Queue::Pause { config, queue } => {
            let config = peren_config::FleetConfig::from_path(config)?.validate()?;
            let report =
                peren_node::queue_pause(&config, &peren_node::QueuePause { queue }).await?;
            println!("queue {} paused changed={}", report.queue, report.changed);
        }
        cli::Queue::Resume { config, queue } => {
            let config = peren_config::FleetConfig::from_path(config)?.validate()?;
            let report =
                peren_node::queue_resume(&config, &peren_node::QueueResume { queue }).await?;
            println!("queue {} resumed changed={}", report.queue, report.changed);
        }
        cli::Queue::Purge { config, queue } => {
            let config = peren_config::FleetConfig::from_path(config)?.validate()?;
            let report =
                peren_node::queue_purge(&config, &peren_node::QueuePurge { queue }).await?;
            println!(
                "queue {} purged queued={} leased={}",
                report.queue, report.queued, report.leased
            );
        }
        cli::Queue::Redrive {
            config,
            source,
            target,
        } => {
            let config = peren_config::FleetConfig::from_path(config)?.validate()?;
            let report =
                peren_node::queue_redrive(&config, &peren_node::QueueRedrive { source, target })
                    .await?;
            println!(
                "queue {} redriven to {} moved={}",
                report.source, report.target, report.moved
            );
        }
    }
    Ok(())
}

pub(super) async fn storage(command: cli::Storage) -> Result<(), CliError> {
    match command {
        cli::Storage::Prune { config, dry_run } => {
            let config = peren_config::FleetConfig::from_path(config)?.validate()?;
            let report = peren_node::prune_storage(
                config,
                &ProcessEnvironment,
                peren_node::StoragePrune { dry_run },
            )
            .await?;
            let action = if report.dry_run {
                "would prune"
            } else {
                "pruned"
            };
            println!(
                "{action} {} replica object{} across {} cell{} ({} byte{}) retained_objects={}",
                report.objects_removed,
                suffix(report.objects_removed),
                report.cells_scanned,
                suffix(report.cells_scanned),
                report.bytes_removed,
                suffix_u64(report.bytes_removed),
                report.objects_retained
            );
        }
        cli::Storage::DeleteCell {
            config,
            cell,
            dry_run,
            force,
        } => {
            if !dry_run && !force {
                return Err(CliError::Unsupported(
                    "storage delete-cell requires --force unless --dry-run is set",
                ));
            }
            let cell = parse_cell_id(&cell)?;
            let config = peren_config::FleetConfig::from_path(config)?.validate()?;
            let report = peren_node::delete_cell_storage(
                config,
                &ProcessEnvironment,
                peren_node::StorageDeleteCell { cell, dry_run },
            )
            .await?;
            let action = if report.dry_run {
                "would delete"
            } else {
                "deleted"
            };
            println!(
                "{action} {} replica object{} for cell {} ({} byte{})",
                report.objects_removed,
                suffix(report.objects_removed),
                report.cell,
                report.bytes_removed,
                suffix_u64(report.bytes_removed)
            );
        }
    }
    Ok(())
}

fn parse_cell_id(value: &str) -> Result<peren_primitives::CellId, CliError> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(CliError::Unsupported(
            "cell id must be 64 hexadecimal characters",
        ));
    }
    let mut bytes = [0_u8; 32];
    for index in 0..32 {
        bytes[index] = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| CliError::Unsupported("cell id must be 64 hexadecimal characters"))?;
    }
    Ok(peren_primitives::CellId::from_bytes(bytes))
}

pub(super) fn kv(command: cli::Kv) -> Result<(), CliError> {
    match command {
        cli::Kv::BulkImport {
            config,
            service,
            binding,
            file,
        } => {
            let config = peren_config::FleetConfig::from_path(config)?.validate()?;
            let report = peren_node::import_kv(
                &config,
                &ProcessEnvironment,
                &peren_node::KvImport {
                    service,
                    binding,
                    file,
                },
            )?;
            println!("imported {} entr{}", report.entries, plural(report.entries));
        }
    }
    Ok(())
}

fn suffix(count: usize) -> &'static str {
    if count == 1 { "" } else { "s" }
}

fn suffix_u64(count: u64) -> &'static str {
    if count == 1 { "" } else { "s" }
}

fn plural(count: usize) -> &'static str {
    if count == 1 { "y" } else { "ies" }
}
