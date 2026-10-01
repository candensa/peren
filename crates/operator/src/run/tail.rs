use crate::{
    cli,
    run::{CliError, ProcessEnvironment},
};

pub(super) fn run(command: cli::Tail) -> Result<(), CliError> {
    let config = peren_config::FleetConfig::from_path(command.config)?.validate()?;
    let report = peren_node::read_tail(
        &config,
        &ProcessEnvironment,
        &peren_node::TailRead {
            service: command.service,
            level: command.level.map(|level| match level {
                cli::TailLevel::Log => peren_node::TailLevel::Log,
                cli::TailLevel::Warn => peren_node::TailLevel::Warn,
                cli::TailLevel::Error => peren_node::TailLevel::Error,
            }),
        },
    )?;
    for event in report.events {
        match event {
            peren_node::TailLogEvent::Request(event) => println!(
                "{} {} {} status={} outcome={} wall_time_ms={}",
                event.service,
                event.method,
                event.path,
                event.status,
                event.outcome,
                event.wall_time_ms
            ),
            peren_node::TailLogEvent::Console(event) => println!(
                "{} console {:?} {}",
                event.service, event.level, event.message
            ),
        }
    }
    Ok(())
}
