use crate::{FleetConfig, Problem, QueueBroker};

use super::required;

pub(super) fn validate(config: &FleetConfig, problems: &mut Vec<Problem>) {
    let Some(queues) = &config.queues else {
        return;
    };
    let endpoint = match queues.broker {
        QueueBroker::Memory | QueueBroker::Cell => return,
        QueueBroker::File => queues.file_path.as_ref(),
        QueueBroker::Nats => queues.nats_url.as_ref(),
        QueueBroker::RabbitMq => queues.amqp_url.as_ref(),
        QueueBroker::Kafka => queues.kafka_bootstrap_servers.as_ref(),
    };
    required(endpoint, "queues connection", problems);
}
