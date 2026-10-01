use super::*;

#[test]
fn external_adapters_own_the_full_queue_lifecycle() {
    for adapter in [Adapter::Nats, Adapter::RabbitMq, Adapter::Kafka] {
        assert!(
            adapter.capability().complete(),
            "{adapter:?} must own dispatch and admin queue lifecycle"
        );
    }
}

#[test]
fn adapter_contract_supports_memory_and_external_brokers() {
    assert_eq!(
        AdapterSpec::memory().validate().unwrap(),
        AdapterStatus::Supported(AdapterCapability::FULL)
    );
    assert_eq!(
        AdapterSpec::external(Adapter::Nats, "nats://localhost:4222")
            .validate()
            .unwrap(),
        AdapterStatus::Supported(AdapterCapability::FULL)
    );
    assert_eq!(
        AdapterSpec::external(Adapter::RabbitMq, "amqp://localhost:5672")
            .validate()
            .unwrap(),
        AdapterStatus::Supported(AdapterCapability::FULL)
    );
    assert_eq!(
        AdapterSpec::external(Adapter::Kafka, "localhost:9092")
            .validate()
            .unwrap(),
        AdapterStatus::Supported(AdapterCapability::FULL)
    );
}

#[test]
fn adapter_contract_validates_endpoint_shape() {
    assert!(matches!(
        AdapterSpec::external(Adapter::Nats, "http://localhost")
            .validate()
            .unwrap_err(),
        QueueError::InvalidEndpoint {
            adapter: Adapter::Nats,
            expected: "nats://"
        }
    ));
    assert!(matches!(
        AdapterSpec {
            adapter: Adapter::RabbitMq,
            endpoint: None
        }
        .validate()
        .unwrap_err(),
        QueueError::MissingEndpoint(Adapter::RabbitMq)
    ));
}
