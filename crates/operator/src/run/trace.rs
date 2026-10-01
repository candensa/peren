use crate::{
    cli,
    run::{CliError, ProcessEnvironment},
};

pub(super) fn run(command: cli::Trace) -> Result<(), CliError> {
    let config = peren_config::FleetConfig::from_path(command.config)?.validate()?;
    let report = peren_node::read_trace(
        &config,
        &ProcessEnvironment,
        &peren_node::TraceRead {
            service: command.service,
            trace_id: command.id,
            request_id: command.request_id,
            limit: command.limit,
        },
    )?;
    if command.json {
        println!("{}", serde_json::to_string_pretty(&report.events)?);
        return Ok(());
    }
    for event in report.events {
        println!(
            "{} {} span={} parent={} cell={} outcome={} duration_ms={}",
            event.service,
            event.name,
            event.span_id,
            event.parent_span_id.as_deref().unwrap_or("-"),
            event.cell.as_deref().unwrap_or("-"),
            event.outcome,
            event.duration_ms
        );
    }
    Ok(())
}
