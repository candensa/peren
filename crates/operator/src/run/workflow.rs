use crate::{
    cli,
    run::{CliError, ProcessEnvironment},
};

pub(super) fn run(command: cli::Workflow) -> Result<(), CliError> {
    match command {
        cli::Workflow::Status(workflow) => {
            let (config, target) = target(workflow)?;
            let report = peren_node::workflow_status(&config, &ProcessEnvironment, target)?;
            print(report.state);
        }
        cli::Workflow::Cancel { workflow, reason } => {
            let (config, target) = target(workflow)?;
            let report = peren_node::cancel_workflow(
                &config,
                &ProcessEnvironment,
                peren_node::WorkflowCancel { target, reason },
            )?;
            print(report.state);
        }
        cli::Workflow::Delete(workflow) => {
            let (config, target) = target(workflow)?;
            let report = peren_node::delete_workflow(&config, &ProcessEnvironment, target)?;
            print(report.state);
        }
    }
    Ok(())
}

fn target(
    workflow: cli::WorkflowId,
) -> Result<(peren_config::ValidatedConfig, peren_node::WorkflowTarget), CliError> {
    let config = peren_config::FleetConfig::from_path(workflow.config)?.validate()?;
    Ok((
        config,
        peren_node::WorkflowTarget {
            service: workflow.service,
            binding: workflow.binding,
            instance: workflow.instance_id,
        },
    ))
}

fn print(state: peren_node::WorkflowState) {
    let status = match state.status {
        peren_node::WorkflowStatus::Unknown => "unknown",
        peren_node::WorkflowStatus::Canceled => "canceled",
        peren_node::WorkflowStatus::Deleted => "deleted",
    };
    print!(
        "workflow {} {} {} status={}",
        state.service, state.binding, state.instance, status
    );
    if let Some(reason) = state.reason {
        print!(" reason={reason}");
    }
    println!();
}
