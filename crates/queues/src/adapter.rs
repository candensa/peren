use crate::QueueError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Adapter {
    Memory,
    Cell,
    Nats,
    RabbitMq,
    Kafka,
}

impl Adapter {
    #[must_use]
    pub const fn supported(self) -> bool {
        self.capability().dispatch_complete()
    }

    #[must_use]
    pub const fn capability(self) -> AdapterCapability {
        match self {
            Self::Memory | Self::Cell | Self::Nats | Self::RabbitMq | Self::Kafka => {
                AdapterCapability::FULL
            }
        }
    }

    #[must_use]
    pub const fn expected_scheme(self) -> Option<&'static str> {
        match self {
            Self::Nats => Some("nats://"),
            Self::RabbitMq => Some("amqp://"),
            Self::Memory | Self::Cell | Self::Kafka => None,
        }
    }

    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Cell => "cell",
            Self::Nats => "nats",
            Self::RabbitMq => "rabbitmq",
            Self::Kafka => "kafka",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterSpec {
    pub adapter: Adapter,
    pub endpoint: Option<String>,
}

impl AdapterSpec {
    #[must_use]
    pub fn memory() -> Self {
        Self {
            adapter: Adapter::Memory,
            endpoint: None,
        }
    }

    pub fn external(adapter: Adapter, endpoint: impl Into<String>) -> Self {
        Self {
            adapter,
            endpoint: Some(endpoint.into()),
        }
    }

    pub fn validate(&self) -> Result<AdapterStatus, QueueError> {
        if matches!(self.adapter, Adapter::Memory | Adapter::Cell) {
            return Ok(AdapterStatus::Supported(self.adapter.capability()));
        }
        let endpoint = self
            .endpoint
            .as_deref()
            .ok_or(QueueError::MissingEndpoint(self.adapter))?;
        if endpoint.trim().is_empty() {
            return Err(QueueError::MissingEndpoint(self.adapter));
        }
        if let Some(scheme) = self.adapter.expected_scheme()
            && !endpoint.starts_with(scheme)
        {
            return Err(QueueError::InvalidEndpoint {
                adapter: self.adapter,
                expected: scheme,
            });
        }
        if self.adapter.supported() {
            Ok(AdapterStatus::Supported(self.adapter.capability()))
        } else {
            Ok(AdapterStatus::Missing(self.adapter.capability()))
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdapterStatus {
    Supported(AdapterCapability),
    Missing(AdapterCapability),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "public adapter capability matrix uses explicit feature flags"
)]
pub struct AdapterCapability {
    pub send: bool,
    pub lease: bool,
    pub acknowledge: bool,
    pub retry: bool,
    pub dead_letter: bool,
    pub pause: bool,
    pub purge: bool,
    pub redrive: bool,
}

impl AdapterCapability {
    pub const FULL: Self = Self {
        send: true,
        lease: true,
        acknowledge: true,
        retry: true,
        dead_letter: true,
        pause: true,
        purge: true,
        redrive: true,
    };

    pub const DISPATCH: Self = Self {
        send: true,
        lease: true,
        acknowledge: true,
        retry: true,
        dead_letter: true,
        pause: false,
        purge: false,
        redrive: false,
    };

    pub const NONE: Self = Self {
        send: false,
        lease: false,
        acknowledge: false,
        retry: false,
        dead_letter: false,
        pause: false,
        purge: false,
        redrive: false,
    };

    #[must_use]
    pub const fn dispatch_complete(self) -> bool {
        self.send && self.lease && self.acknowledge && self.retry && self.dead_letter
    }

    #[must_use]
    pub const fn admin_complete(self) -> bool {
        self.pause && self.purge && self.redrive
    }

    #[must_use]
    pub const fn complete(self) -> bool {
        self.dispatch_complete() && self.admin_complete()
    }
}
